package ams.exp.market_simulation

import ams.exp.market_simulation.MarketSimulationConfigProtos.{BaseParam, ParamInfo}
import ams.exp.market_simulation.MarketSimulationProtos._
import ams.exp.market_simulation.module.ad_strategy.AdStrategySimulation
import ams.exp.market_simulation.module.display.{DisplaySimulation, PredictModelSimulation}
import ams.exp.market_simulation.module.effect.EffectSimulation
import ams.exp.market_simulation.module.element.{AdBehaviourSimulation, ElementSimulation}
import ams.exp.market_simulation.module.util.Util
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import ucube.common.scala.TdwHelper.saveDataToTdw

object MarketSimulationMain extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def updateParamInfo(orgPi: ParamInfo, beginPartition: Long, seed: Int) = {
    val partitionDate = if (beginPartition == 0L) {
      orgPi.getBaseParam.getPartitionDate
    } else {
      (beginPartition + seed).toString
    }
    val newSeed = orgPi.getBaseParam.getRandomSeed + seed
    ParamInfo.newBuilder(orgPi).setBaseParam {
      BaseParam.newBuilder(orgPi.getBaseParam)
        .setRandomSeed(newSeed)
        .setPartitionDate(partitionDate)
        .setTestName(partitionDate)
    }.build()
  }

  def process(beginPartition: Long, seed: Int)(implicit sc: SparkContext): Unit = {
    println("repeatTimes:" + seed)
    val orgPi: ParamInfo = Util.initParamInfo
    val pi = updateParamInfo(orgPi, beginPartition, seed)
    print("pi:" + pi)
    val param = pi.getBaseParam
    implicit val piBc: ParamInfoBc = sc.broadcast(pi)
    implicit val printDetail: Boolean = param.getPrintDetail
    val outputPartitionNum = param.getOutputPartitionNum
    val testPath = s"${param.getBaseHdfsPath}/${param.getTestName}"

    if (param.getCreateNewTest) {
      Util.removeHdfsPath(testPath)
    }
    // 主流程参考文档：https://docs.qq.com/doc/DUFZSZHJrb2VJeWNL
    Range(0, (param.getReqCnt / param.getReqCntPerBatch).toInt).toList.foreach { roundNo =>
      val lastReqId = Util.readLastReqIdFromCheckpoint(s"$testPath/request_record")
      // 0.1: 对象模拟，初始化用户、广告、模型信息，或者读取已经存在的用户、广告、模型信息
      val (userInfo, adInfo, modelInfo) = if (param.getCreateNewTest && lastReqId == 0L) {
        (ElementSimulation.initUserInfo.cache(),
          ElementSimulation.initAdInfo.cache(),
          PredictModelSimulation.initModelInfo)
      } else {
        (ElementSimulation.readUserInfoFromCheckpoint(s"$testPath/user_current_info").cache(),
          ElementSimulation.readAdInfoFromCheckpoint(s"$testPath/ad_current_info").cache(),
          PredictModelSimulation.readModelInfoFromCheckpoint(s"$testPath/model_info"))
      }
      val modelInfoBc = sc.broadcast(modelInfo)
      println(s"lastReqId:$lastReqId")
      Util.printRddDetail("userInfo", userInfo) { r: UserCurrentInfo => r.getUserId }
      Util.printRddDetail("adInfo", adInfo) { r: AdCurrentInfo => r.getAdId }
      // 每个batch会模拟reqCntPerBatch条请求，分批进行模拟：
      if (lastReqId < param.getReqCnt) {
        // 1: 播放模拟，根据用户信息和广告信息，模拟用户发起请求时播放系统经过召回排序等过程得到胜出广告，并将相关信息记录在trackLogInfo中
        val trackLogInfo = DisplaySimulation.runSimulation(userInfo, adInfo, modelInfoBc, lastReqId, roundNo).cache()
        Util.saveCheckPoint(trackLogInfo.map(_.toByteArray), s"$testPath/track_log_info/$roundNo", outputPartitionNum)
        Util.printRddDetail("trackLogInfo", trackLogInfo) { r: RequestInfo => r.getRequestId }

        // 2.1: 效果模拟，根据trackLogInfo中的信息，模拟每条请求中胜出广告的播放效果，包括点击、转化、消耗等效果，并将每条请求的效果记录在effectInfo中
        val effectInfo = EffectSimulation.runSimulation(trackLogInfo).cache()
        Util.saveCheckPoint(effectInfo.map(_.toByteArray), s"$testPath/effect_info/$roundNo", outputPartitionNum)
        Util.printRddDetail("effectInfo", effectInfo) { r: RequestInfo => r.getRequestId }

        // 2.2: 根据effectInfo中记录的效果，经过聚合等逻辑，最后关联到对应的用户及广告上，更新用户和广告的效果数据
        val (newUserInfo, newAdInfo) = EffectSimulation.updateMetricByEffect(userInfo, adInfo, effectInfo, roundNo)
        Util.saveCheckPoint(newUserInfo.map(_.toByteArray), s"$testPath/user_current_info/$roundNo", outputPartitionNum)
        Util.printRddDetail("newUserInfo", newUserInfo) { r: UserCurrentInfo => r.getUserId }

        // 3: 广告主行为模拟，根据更新效果后的广告信息，模拟广告主的暂停行为
        val newAdInfo1 = AdBehaviourSimulation.runSimulation(newAdInfo, roundNo).cache()
        Util.printRddDetail("newAdInfo1", newAdInfo1) { r: AdCurrentInfo => r.getAdId }

        // 4: 广告策略模拟，根据更新效果后的广告信息，更新广告的策略信息，包括调价因子等信息，输出并记录在newAdInfo2中
        val newAdInfo2 = AdStrategySimulation.runSimulation(newAdInfo1).cache()
        Util.saveCheckPoint(newAdInfo2.map(_.toByteArray), s"$testPath/ad_current_info/$roundNo", outputPartitionNum)
        Util.printRddDetail("newAdInfo2", newAdInfo2) { r: AdCurrentInfo => r.getAdId }

        // 5: 预测模型更新，读取历史全部效果，对模型进行更新，输出并记录在modelInfo中
        val fullEffectInfo = Util.readRecordIO(s"$testPath/effect_info/*").map(RequestInfo.parseFrom).cache()
        val newModelInfo = PredictModelSimulation.updateModel(fullEffectInfo)
        Util.saveCheckPoint(sc.parallelize(List(newModelInfo)).map(_.toByteArray), s"$testPath/model_info/$roundNo", 1)
        println(s"newModelInfo:$newModelInfo")

        // 6: 记录当前模拟的最后一个请求id，下次执行的batch将从这个id开始，继续模拟
        Util.saveReqIdToCheckpoint(lastReqId + param.getReqCntPerBatch, s"$testPath/request_record")
      }
    }
    val effectInfo = Util.readRecordIO(s"$testPath/effect_info/*").map(RequestInfo.parseFrom).cache()
    val effectData = EffectSimulation.extractEffectInfo(effectInfo, param.getPartitionDate).repartition(outputPartitionNum)
    saveDataToTdw(effectData, param.getPartitionDate, param.getOutputDbName, param.getOutputTblName, true)
    if (param.getRmCheckpoint) {
      Util.removeHdfsPath(testPath)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MarketSimulationMain")
    implicit val sc: SparkContext = new SparkContext(sparkConf)
    process(0, 0)
  }
}
