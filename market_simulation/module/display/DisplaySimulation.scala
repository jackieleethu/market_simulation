package ams.exp.market_simulation.module.display

import java.util.Random

import ams.exp.market_simulation.MarketSimulationConfigProtos.DisplayParam.ExpType
import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo.{AdRunningStatus, AdScoreInfo}
import ams.exp.market_simulation.MarketSimulationProtos.{AdCurrentInfo, ModelInfo, RequestInfo, UserCurrentInfo}
import ams.exp.market_simulation.module.effect.EffectSimulation
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object DisplaySimulation extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def getExpCoef(user: UserCurrentInfo, ad: AdCurrentInfo)(implicit pi: ParamInfo): Int = {
    pi.getDisplayParam.getExpType match {
      case ExpType.EXP_TYPE_AD_FLOW_JOINT_TEST =>
        if (user.getExpInfo(0).getExpId + ad.getExpInfo(0).getExpId < 4) 0 else 1
      case ExpType.EXP_TYPE_USER_AB_TEST =>
        user.getExpInfo(0).getExpId
      case ExpType.EXP_TYPE_AD_AB_TEST =>
        ad.getExpInfo(0).getExpId
      case ExpType.EXP_TYPE_SPLIT_UPLIFT_TEST =>
        if (user.getExpInfo(0).getExpId < 5 && ad.getExpInfo(0).getExpId == 0) 1 else 0
      case ExpType.EXP_TYPE_AD_SUPPORT_TEST =>
        if (ad.getExpInfo(0).getExpId < 2) 1 else 0
      case ExpType.EXP_TYPE_AD_SUPPORT_USER_AB_TEST =>
        if (user.getExpInfo(0).getExpId < 5 && ad.getExpInfo(0).getExpId < 2) 1 else 0
      case ExpType.EXP_TYPE_AD_SPLIT_TEST =>
        if (user.getExpInfo(0).getExpId == 1 && ad.getAdId >= pi.getDisplayParam.getBaseAdRange) 1 else 0
      case _ =>
        0
    }
  }

  def calAdRankScore(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, model: ModelInfo)(implicit pi: ParamInfo): AdCurrentInfo = {
    val realCvr = EffectSimulation.getRealCvr(user, ad)
    val cvrBias = PredictModelSimulation.getPrdCvrBias(user, ad)
    val prdCvr = PredictModelSimulation.getRankPrdCvr(reqId, user, ad, model)
    val rankingEcpm = ad.getFeatures.getAdjustFactor * prdCvr * ad.getFeatures.getTargetCpa
    val ecpm = if ((pi.getDisplayParam.getUseSplitUpliftTest ||
      pi.getDisplayParam.getUseAdSupport) && DisplaySimulation.getExpCoef(user, ad) > 0) {
      rankingEcpm / (pi.getPredictModelParam.getCvrBiasLift + 1.0)
    } else {
      rankingEcpm
    }
    AdCurrentInfo.newBuilder(ad).setAdScoreInfo(
      AdScoreInfo.newBuilder().setRealCvr(realCvr).setCvrBias(cvrBias).setPrdCvr(prdCvr).setEcpm(ecpm).setRankingEcpm(rankingEcpm)
    ).build()
  }

  def calAdRerankScore(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, model: ModelInfo)(implicit pi: ParamInfo): AdCurrentInfo = {
    val realCvr = EffectSimulation.getRealCvr(user, ad)
    val cvrBias = PredictModelSimulation.getPrdCvrBias(user, ad)
    val prdCvr = PredictModelSimulation.getRerankPrdCvr(reqId, user, ad, model)
    val rankingEcpm = ad.getFeatures.getAdjustFactor * prdCvr * ad.getFeatures.getTargetCpa
    val ecpm = if ((pi.getDisplayParam.getUseSplitUpliftTest ||
      pi.getDisplayParam.getUseAdSupport) && DisplaySimulation.getExpCoef(user, ad) > 0) {
      rankingEcpm / (pi.getPredictModelParam.getCvrBiasLift + 1.0)
    }
    else {
      rankingEcpm
    }
    AdCurrentInfo.newBuilder(ad).setAdScoreInfo(
      AdScoreInfo.newBuilder().setRealCvr(realCvr).setCvrBias(cvrBias).setPrdCvr(prdCvr).setEcpm(ecpm).setRankingEcpm(rankingEcpm)
    ).build()
  }

  def getRankedAdList(reqId: Long, user: UserCurrentInfo, adInfoList: List[AdCurrentInfo], model: ModelInfo)
                     (implicit pi: ParamInfo): List[AdCurrentInfo] = {
    adInfoList.map(calAdRankScore(reqId, user, _, model)).sortBy(-1.0 * _.getAdScoreInfo.getRankingEcpm)
      .take(pi.getDisplayParam.getInRerankingAdNum)
  }

  def getRerankedAdList(reqId: Long, user: UserCurrentInfo, adInfoList: List[AdCurrentInfo], model: ModelInfo)
                       (implicit pi: ParamInfo): List[AdCurrentInfo] = {
    adInfoList.map(calAdRerankScore(reqId, user, _, model)).sortBy(-1.0 * _.getAdScoreInfo.getRankingEcpm)
  }

  def getWinAdIndex(user: UserCurrentInfo, adList: List[AdCurrentInfo])(implicit pi: ParamInfo) = {
    lazy val newIdx = adList.indexWhere(_.getExpInfo(0).getExpId > 1)
    if ((pi.getDisplayParam.getUseSplitUpliftTest || pi.getDisplayParam.getUseUpliftTest) &&
      user.getExpInfo(0).getExpId % 5 < pi.getDisplayParam.getSplitUpliftTestHoldoutRange && newIdx > 0) {
      newIdx
    } else {
      0
    }
  }

  def getTriggerValue(user: UserCurrentInfo, adList: List[AdCurrentInfo])(implicit pi: ParamInfo) = {
    if (user.getExpInfo(0).getExpId < 5) {
      adList.sortBy(-1.0 * _.getAdScoreInfo.getEcpm).headOption.map(_.getExpInfo(0).getExpId).getOrElse(-1)
    } else {
      adList.sortBy { ad =>
        if (ad.getExpInfo(0).getExpId <= 1)
          -1.0 * ad.getAdScoreInfo.getEcpm * (1.0 + pi.getPredictModelParam.getCvrBiasLift)
        else
          -1.0 * ad.getAdScoreInfo.getEcpm
      }.headOption.map(_.getExpInfo(0).getExpId).getOrElse(-1)
    }
  }

  def runSimulation(userInfo: RDD[UserCurrentInfo], adInfo: RDD[AdCurrentInfo], modelInfoBc: Broadcast[ModelInfo],
                    baseReqId: Long, roundNo: Int)(implicit sc: SparkContext, piBc: ParamInfoBc): RDD[RequestInfo] = {
    val pi = piBc.value
    val rand = new Random(baseReqId + 30000 + pi.getBaseParam.getRandomSeed * pi.getBaseParam.getReqCnt)
    val randomRdd = sc.parallelize(Range(0, pi.getBaseParam.getReqCntPerBatch).map { i =>
      implicit val pi = piBc.value
      val reqId = baseReqId + i
      val randomId = rand.nextLong() % pi.getBaseParam.getUserCnt
      val userId = if (randomId >= 0) randomId else randomId + pi.getBaseParam.getUserCnt
      (userId, reqId)
    })
    val userRdd = userInfo.map(user => (user.getUserId, user))
    val joinRdd = randomRdd.join(userRdd).repartition(pi.getBaseParam.getRepartitionNum)
    val adInfoList = adInfo.filter(_.getAdRunningStatus.equals(AdRunningStatus.RUNNING)).collect().toList
    val adInfoListBc = sc.broadcast(adInfoList)
    joinRdd.map {
      case (_, (reqId, user)) =>
        implicit val pi = piBc.value
        val adInfoList = adInfoListBc.value
        val rand = new Random(reqId + 40000 + user.getUserId)
        val p0 = rand.nextDouble()
        val p = rand.nextDouble()
        val filterAdInfoList = adInfoList.filter { ad =>
          lazy val filterByNewAdSplit = pi.getDisplayParam.getUseNewAdSplit &&
            ((p > pi.getDisplayParam.getNewAdSplitRatio && ad.getFeatures.getIsNewAd) ||
              (p < pi.getDisplayParam.getNewAdSplitRatio && !ad.getFeatures.getIsNewAd))
          lazy val filterByTargetingLogic = pi.getDisplayParam.getUseTargetingLogic &&
            !user.getFeatures.getUserTargetingList.zip(ad.getFeatures.getAdTargetingList).map {
              case (ut, at) => at == -1 || ut == at
            }.reduceOption(_ && _).getOrElse(false)
          lazy val filterByBudgetLogic = pi.getDisplayParam.getUseBudgetLogic &&
            ad.getFeatures.getRemainBudget <= 0
          lazy val filterBySplitUpliftTest = pi.getDisplayParam.getUseSplitUpliftTest &&
            ((user.getExpInfo(0).getExpId < 5 && ad.getExpInfo(0).getExpId == 1) ||
              (user.getExpInfo(0).getExpId >= 5 && ad.getExpInfo(0).getExpId == 0))
          lazy val filterBySplitUpliftTestV2 = pi.getDisplayParam.getUseSplitUpliftTestV2 &&
            ((user.getExpInfo(0).getExpId < 5 && ad.getExpInfo(0).getExpId % 2 == 1) ||
              (user.getExpInfo(0).getExpId >= 5 && ad.getExpInfo(0).getExpId % 2 == 0))
          lazy val filterByAdCopySplit = pi.getDisplayParam.getUseAdCopySplit &&
            ((user.getExpInfo(0).getExpId == 0 && ad.getAdId < pi.getBaseParam.getAdCnt - pi.getBaseParam.getCopyAdCnt) ||
              (user.getExpInfo(0).getExpId == 1 && ad.getAdId >= pi.getBaseParam.getAdCnt - pi.getBaseParam.getCopyAdCnt))
          !filterByNewAdSplit && !filterByTargetingLogic && !filterByBudgetLogic && !filterBySplitUpliftTest &&
            !filterBySplitUpliftTestV2 && !filterByAdCopySplit
        }
        if (pi.getDisplayParam.getUseTwoStepRanking) {
          val rankedAdList = getRankedAdList(reqId, user, filterAdInfoList, modelInfoBc.value)
          val rerankedAdList = getRerankedAdList(reqId, user, rankedAdList, modelInfoBc.value)
          RequestInfo.newBuilder()
            .setRequestId(reqId)
            .setRequestTime(roundNo)
            .setUserCurrentInfo(user)
            .addAllRankingAdInfo(rankedAdList.take(pi.getDisplayParam.getTrackLogAdTakenNum))
            .addAllRerankingAdInfo(rerankedAdList.take(pi.getDisplayParam.getTrackLogAdTakenNum))
            .setWinAdIndex(getWinAdIndex(user, rerankedAdList))
            .setTriggerValue(getTriggerValue(user, rerankedAdList))
            .build()
        } else {
          val rankedAdList = getRankedAdList(reqId, user, filterAdInfoList, modelInfoBc.value)
          RequestInfo.newBuilder()
            .setRequestId(reqId)
            .setRequestTime(roundNo)
            .setUserCurrentInfo(user)
            .addAllRerankingAdInfo(rankedAdList.take(pi.getDisplayParam.getTrackLogAdTakenNum))
            .setWinAdIndex(getWinAdIndex(user, rankedAdList))
            .setTriggerValue(getTriggerValue(user, rankedAdList))
            .build()
        }
    }
  }
}
