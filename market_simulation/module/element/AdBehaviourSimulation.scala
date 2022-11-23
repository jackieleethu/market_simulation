package ams.exp.market_simulation.module.element


import java.util.Random

import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo.AdFeatures
import ams.exp.market_simulation.MarketSimulationProtos.{AdCurrentInfo, ExpInfo}
import ams.exp.market_simulation.module.util.Util
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object AdBehaviourSimulation extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def runSimulation(adInfo: RDD[AdCurrentInfo], roundNo: Int)(implicit sc: SparkContext, piBc: ParamInfoBc): RDD[AdCurrentInfo] = {
    val pi = piBc.value
    if (pi.getAdBehaviourParam.getUseAdBehaviour) {
      val orgAdInfo = adInfo.map { ad =>
        implicit val pi = piBc.value.getAdBehaviourParam
        val gmv = ad.getMetrics.getGmv
        val cost = ad.getMetrics.getCost
        val convertion = ad.getMetrics.getConversion
        val imp = ad.getMetrics.getImpression
        val runningStatus = ad.getAdRunningStatus
        val overRatio = if (gmv > 0) cost / gmv - 1 else 0
        val adId = ad.getAdId
        val newRunningStatus = if (runningStatus == AdCurrentInfo.AdRunningStatus.RUNNING &&
          convertion > pi.getAdPauseMinConversion &&
          (overRatio < pi.getAdPauseMinOverRatio || overRatio > pi.getAdPauseMaxOverRatio)) {
          AdCurrentInfo.AdRunningStatus.PAUSE
        } else {
          runningStatus
        }
        AdCurrentInfo.newBuilder(ad).setAdRunningStatus(newRunningStatus).build()
      }
      if (pi.getAdBehaviourParam.getCreateNewAd) {
        val rand = new Random(roundNo + 40000 + pi.getBaseParam.getRandomSeed)
        val newAdInfo = orgAdInfo.filter(_.getAdRunningStatus.equals(AdCurrentInfo.AdRunningStatus.PAUSE)).collect()
          .flatMap { ad =>
            implicit val pi = piBc.value
            val id = ad.getAdId + (roundNo + 1) * pi.getBaseParam.getAdCnt
            val newAd = ElementSimulation.getNewAdPattern(id, rand)
            val orgAd = AdCurrentInfo.newBuilder(ad)
              .setAdRunningStatus(AdCurrentInfo.AdRunningStatus.STOP)
              .build()
            List(newAd, orgAd)
          }
        sc.parallelize(newAdInfo).union(orgAdInfo)
      } else {
        orgAdInfo.map { ad =>
          if (ad.getAdRunningStatus.equals(AdCurrentInfo.AdRunningStatus.PAUSE)) {
            AdCurrentInfo.newBuilder(ad).setAdRunningStatus(AdCurrentInfo.AdRunningStatus.STOP).build()
          } else {
            ad
          }
        }
      }
    } else if (pi.getAdBehaviourParam.getUseAdFeatureChange && roundNo == pi.getAdBehaviourParam.getAdFeatureChangeRoundNo) {
      println("UseAdFeatureChange,roundNo=" + roundNo)
      implicit val printDetail: Boolean = true
      Util.printRddDetail("orgAdInfo", adInfo) { r: AdCurrentInfo => r.getAdId }
      val rand = new Random(roundNo + 40000 + pi.getBaseParam.getRandomSeed)
      val newAdInfo = adInfo.map { ad =>
        implicit val pi = piBc.value
        val adId = ad.getAdId
        if ((adId % 100) < pi.getAdBehaviourParam.getAdFeatureChangeRange) {
          val newFeature = ElementSimulation.getNewAdPattern(adId, rand).getFeatures.getAdFeatureList
          AdCurrentInfo.newBuilder(ad).setFeatures(AdFeatures.newBuilder(ad.getFeatures).clearAdFeature().addAllAdFeature(newFeature)).build()
        } else {
          ad
        }
      }
      Util.printRddDetail("newAdInfo", newAdInfo) { r: AdCurrentInfo => r.getAdId }
      newAdInfo
    } else {
      adInfo
    }
  }
}
