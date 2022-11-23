package ams.exp.market_simulation

import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos._
import ams.exp.market_simulation.module.effect.EffectSimulation
import ams.exp.market_simulation.module.util.Util
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import ucube.common.scala.TdwHelper._

object MarketSimulationAnalysisMain extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val pi: ParamInfo = Util.initParamInfo

    val sparkConf = new SparkConf().setAppName("MarketSimulationAnalysisMain")
    implicit val sc: SparkContext = new SparkContext(sparkConf)
    implicit val piBc: ParamInfoBc = sc.broadcast(pi)

    val param = pi.getBaseParam
    val effectInfo = Util.readRecordIO(s"${param.getBaseHdfsPath}/${param.getTestName}/effect_info/*").map(RequestInfo.parseFrom).cache()
    val ret = EffectSimulation.extractEffectInfo(effectInfo, param.getPartitionDate).repartition(param.getOutputPartitionNum)
    saveDataToTdw(ret, param.getPartitionDate, param.getOutputDbName, param.getOutputTblName, true)
  }
}
