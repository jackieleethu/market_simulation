package ams.exp.market_simulation.module.effect

import java.util.Random

import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo.AdFeatures
import ams.exp.market_simulation.MarketSimulationProtos.{AdCurrentInfo, Metrics, RequestInfo, UserCurrentInfo}
import ams.exp.market_simulation.module.display.{DisplaySimulation, PredictModelSimulation}
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import ucube.common.scala.TypeFunctorInstance._
import ucube.common.scala.Util.tabulateAll

import scala.collection.JavaConversions._

object EffectSimulation extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def mergeMetrics(a: Metrics, b: Metrics): Metrics = {
    Metrics.newBuilder
      .setImpression(a.getImpression + b.getImpression)
      .setClick(a.getClick + b.getClick)
      .setConversion(a.getConversion + b.getConversion)
      .setCost(a.getCost + b.getCost)
      .setGmv(a.getGmv + b.getGmv)
      .build()
  }

  def getRealCvr(user: UserCurrentInfo, ad: AdCurrentInfo)(implicit pi: ParamInfo): Double = {
    val param = pi.getEffectParam
    //    val temp = (param.getRealCvrUserCoefList.split(",").zip(user.getFeatures.getUserFeatureList) ++
    //      param.getRealCvrAdCoefList.split(",").zip(ad.getFeatures.getAdFeatureList)).map(t => t._1.toDouble * t._2).sum
    val temp = if (pi.getPredictModelParam.getUseInteractions) {
      param.getRealCvrUserCoefList.split(",")
        .zip(user.getFeatures.getUserFeatureList)
        .zip(ad.getFeatures.getAdFeatureList)
        .map {
          case ((coef, userFeature), adFeature) =>
            coef.toDouble * userFeature * adFeature
        }.sum
    } else {
      (param.getRealCvrUserCoefList.split(",").zip(user.getFeatures.getUserFeatureList) ++
        param.getRealCvrAdCoefList.split(",").zip(ad.getFeatures.getAdFeatureList)).map(t => t._1.toDouble * t._2).sum
    }
    val temp2 = if (DisplaySimulation.getExpCoef(user, ad) > 0) {
      1.0 + param.getRealCvrLift
    } else {
      1.0
    }
    (param.getBaseRealCvr + temp) * temp2 * pi.getElementParam.getTargetCpaConst / ad.getFeatures.getTargetCpa
  }

  def runSimulation(trackLogInfo: RDD[RequestInfo])(implicit piBc: ParamInfoBc): RDD[RequestInfo] = {
    trackLogInfo.map { requestInfo =>
      implicit val pi = piBc.value
      val reqId = requestInfo.getRequestId
      val user = requestInfo.getUserCurrentInfo
      val winAdIdx = requestInfo.getWinAdIndex
      val ad = requestInfo.getRerankingAdInfo(winAdIdx)
      val realCvr = getRealCvr(user, ad)
      val rand = new Random(reqId + pi.getBaseParam.getRandomSeed * pi.getBaseParam.getReqCnt)
      val p0 = rand.nextDouble()
      val p = rand.nextDouble()
      val imp = 1
      val conv = if (p <= realCvr) 1 else 0
      val cost = if (pi.getDisplayParam.getUseGspCost && winAdIdx + 1 < pi.getDisplayParam.getInRerankingAdNum) {
        requestInfo.getRerankingAdInfo(winAdIdx + 1).getAdScoreInfo.getEcpm
      } else {
        ad.getAdScoreInfo.getEcpm
      }
      val gmv = conv * ad.getFeatures.getTargetCpa
      RequestInfo.newBuilder(requestInfo)
        .setMetrics(Metrics.newBuilder().setImpression(imp).setConversion(conv).setCost(cost).setGmv(gmv))
        .build()
    }
  }

  def updateMetricByEffect(userInfo: RDD[UserCurrentInfo], adInfo: RDD[AdCurrentInfo], effectInfo: RDD[RequestInfo], roundNo: Int)
                          (implicit piBc: ParamInfoBc): (RDD[UserCurrentInfo], RDD[AdCurrentInfo]) = {
    val userEffect = effectInfo.map { effect =>
      (effect.getUserCurrentInfo.getUserId, effect.getMetrics)
    }.reduceByKey((a, b) => mergeMetrics(a, b))
    val adEffect = effectInfo.map { effect =>
      (effect.getRerankingAdInfo(effect.getWinAdIndex).getAdId, effect.getMetrics)
    }.reduceByKey((a, b) => mergeMetrics(a, b))
    val newUserInfo = userInfo.map(user => (user.getUserId, user)).leftOuterJoin(userEffect).map {
      case (userId, (orgUser, newMetricsOpt)) =>
        newMetricsOpt.map { newMetrics =>
          val metrics = mergeMetrics(orgUser.getMetrics, newMetrics)
          UserCurrentInfo.newBuilder(orgUser).setMetrics(metrics).build()
        }.getOrElse(orgUser)
    }.cache()
    val newAdInfo = adInfo.map(ad => (ad.getAdId, ad)).leftOuterJoin(adEffect).map {
      case (adId, (orgAd, newMetricsOpt)) =>
        newMetricsOpt.map { newMetrics =>
          val metrics = mergeMetrics(orgAd.getMetrics, newMetrics)
          val newBudget = if ((roundNo + 1) % piBc.value.getBaseParam.getBatchNumPerDay != 0) {
            orgAd.getFeatures.getRemainBudget - newMetrics.getCost
          } else {
            orgAd.getFeatures.getTotalBudget
          }
          val isNewAd = orgAd.getFeatures.getIsNewAd && metrics.getConversion < piBc.value.getDisplayParam.getNewAdConvThreshold
          AdCurrentInfo.newBuilder(orgAd).setMetrics(metrics)
            .setFeatures(AdFeatures.newBuilder(orgAd.getFeatures).setIsNewAd(isNewAd).setRemainBudget(newBudget))
            .build()
        }.getOrElse(orgAd)
    }.cache()
    (newUserInfo, newAdInfo)
  }

  def extractEffectKey(requestInfo: RequestInfo, ad: AdCurrentInfo)(implicit pi: ParamInfo) = {
    val reqId = requestInfo.getRequestId
    val user = requestInfo.getUserCurrentInfo
    val uid = user.getUserId
    val aid = ad.getAdId
    val userExpId = user.getExpInfo(0).getExpId
    val adExpId = ad.getExpInfo(0).getExpId
    val targetCpa = ad.getFeatures.getTargetCpa
    val prdCvr = ad.getAdScoreInfo.getPrdCvr
    val adjustFactor = ad.getFeatures.getAdjustFactor
    val ecpm = ad.getAdScoreInfo.getEcpm
    val rankingEcpm = ad.getAdScoreInfo.getRankingEcpm
    val realCvr = ad.getAdScoreInfo.getRealCvr
    val isHitStrategy = DisplaySimulation.getExpCoef(user, ad)
    val isNewAd = if (ad.getFeatures.getIsNewAd) 1 else 0
    val winAdIdx = requestInfo.getWinAdIndex
    val triggerValue = requestInfo.getTriggerValue
    ((reqId, uid, aid, isNewAd, winAdIdx, triggerValue), (userExpId, adExpId, isHitStrategy, targetCpa, prdCvr, adjustFactor, ecpm, rankingEcpm, realCvr))
  }

  def extractEffectInfo(effectInfo: RDD[RequestInfo], partitionDate: String)(implicit piBc: ParamInfoBc): RDD[Array[String]] = {
    effectInfo.flatMap { requestInfo =>
      implicit val pi = piBc.value
      val winAdIdx = requestInfo.getWinAdIndex
      val rerankInfo = requestInfo.getRerankingAdInfoList.zipWithIndex.map {
        case (ad, idx) =>
          val key = extractEffectKey(requestInfo, ad)
          val rangeStage = 2
          val (imp, clk, conv, gmv, cost) = if (idx == winAdIdx) {
            val metrics = requestInfo.getMetrics
            (metrics.getImpression,
              metrics.getClick,
              metrics.getConversion,
              metrics.getGmv,
              metrics.getCost)
          } else {
            (0L, 0L, 0L, 0.0, 0.0)
          }
          ((key, rangeStage, idx), (imp, clk, conv, gmv, cost))
      }.toList
      val rankInfo = requestInfo.getRankingAdInfoList.zipWithIndex.map {
        case (ad, idx) =>
          val key = extractEffectKey(requestInfo, ad)
          val rangeStage = 1
          ((key, rangeStage, idx), (0L, 0L, 0L, 0.0, 0.0))
      }.toList
      rerankInfo ++ rankInfo
    }.map(a => tabulateAll((partitionDate, a)))
  }
}
