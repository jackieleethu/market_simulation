package ams.exp.market_simulation

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object MarketSimulationMainV2 extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MarketSimulationMainV2")
    implicit val sc: SparkContext = new SparkContext(sparkConf)

    val argMap = ucube.common.scala.Util.parseArgs(args)
    val repeatTimes = argMap.getOrElse("repeatTimes", "10").toInt
    val beginPartition = argMap.getOrElse("beginPartition", "2021121000").toLong
    Range(0, repeatTimes).foreach(MarketSimulationMain.process(beginPartition, _))
  }
}
