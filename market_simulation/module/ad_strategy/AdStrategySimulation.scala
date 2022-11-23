package ams.exp.market_simulation.module.ad_strategy

import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo
import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo.{AdFeatures, AdRunningStatus}
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object AdStrategySimulation extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def getAdjustFunctor(biasList: List[Double])(implicit pi: ParamInfo): Double = {
    val param = pi.getAdStrategyParam
    val f = if (biasList.isEmpty) {
      1.0
    } else {
      val p = biasList.last
      val i = biasList.sum
      val d = if (biasList.size > 1) {
        biasList.last - biasList(biasList.size - 2)
      } else {
        0.0
      }
      1.0 / (1.0 + param.getKp * p + param.getKi * i + param.getKd * d)
    }
    if (f > param.getAdjustFactorMax) {
      param.getAdjustFactorMax
    } else if (f < param.getAdjustFactorMin) {
      param.getAdjustFactorMin
    } else f
  }

  def runSimulation(adInfo: RDD[AdCurrentInfo])(implicit piBc: ParamInfoBc): RDD[AdCurrentInfo] = {
    val pi = piBc.value.getDisplayParam
    if (pi.getUseAdjustFactor) {
      adInfo.map { ad =>
        implicit val pi = piBc.value
        if (ad.getAdRunningStatus.equals(AdRunningStatus.RUNNING)) {
          val cpmBiasList = ad.getFeatures.getCpmBiasListList.map(_.toDouble).toList
          val newCpmBiasList = cpmBiasList :+ ((ad.getMetrics.getCost + 5 * ad.getFeatures.getTargetCpa) /
            (ad.getMetrics.getGmv + 5 * ad.getFeatures.getTargetCpa) - 1)
          val newAdjustFactor = getAdjustFunctor(newCpmBiasList)
          val newAdFeatures = AdFeatures.newBuilder(ad.getFeatures)
            .clearCpmBiasList()
            .addAllCpmBiasList(newCpmBiasList.map(Double.box))
            .setAdjustFactor(newAdjustFactor)
          AdCurrentInfo.newBuilder(ad).setFeatures(newAdFeatures).build()
        } else {
          ad
        }
      }
    } else {
      adInfo
    }
  }
}
