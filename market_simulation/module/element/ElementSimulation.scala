package ams.exp.market_simulation.module.element

import java.util.Random

import ams.exp.market_simulation.MarketSimulationConfigProtos.DisplayParam.ExpType
import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos.AdCurrentInfo.AdFeatures
import ams.exp.market_simulation.MarketSimulationProtos.UserCurrentInfo.UserFeatures
import ams.exp.market_simulation.MarketSimulationProtos.{AdCurrentInfo, ExpInfo, UserCurrentInfo}
import ams.exp.market_simulation.module.util.Util
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object ElementSimulation extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def getExpNum(implicit pi: ParamInfo) = {
    pi.getDisplayParam.getExpType match {
      case ExpType.EXP_TYPE_AD_FLOW_JOINT_TEST =>
        (4, 5)
      case ExpType.EXP_TYPE_USER_AB_TEST =>
        (2, 1)
      case ExpType.EXP_TYPE_AD_AB_TEST =>
        (1, pi.getDisplayParam.getAdExpNum)
      case ExpType.EXP_TYPE_SPLIT_UPLIFT_TEST =>
        (10, pi.getDisplayParam.getAdExpNum)
      case ExpType.EXP_TYPE_AD_SUPPORT_TEST =>
        (10, pi.getDisplayParam.getAdExpNum)
      case ExpType.EXP_TYPE_AD_SUPPORT_USER_AB_TEST =>
        (10, pi.getDisplayParam.getAdExpNum)
      case ExpType.EXP_TYPE_AD_SPLIT_TEST =>
        (2, 1)
      case _ =>
        (1, 1)
    }
  }

  def getNewAdPattern(id: Long, rand: Random)(implicit pi: ParamInfo): AdCurrentInfo = {
    val param = pi.getElementParam
    val expId = id % getExpNum(pi)._2
    val adFeatureList = Range(0, param.getRealAdFeatureNum).toList.map { i =>
      val randValue = (rand.nextDouble() - 0.5) * 2.0
      Double.box(randValue)
    }
    val targetCpaOrg = if (param.getTargetCpaMax - param.getTargetCpaMin < 0) {
      param.getTargetCpaMax
    } else {
      rand.nextInt(param.getTargetCpaMax - param.getTargetCpaMin + 1) + param.getTargetCpaMin
    }
    val targetCpa = if (param.getDiffTargetCpa) {
      val adExpNum = pi.getDisplayParam.getAdExpNum
      targetCpaOrg * (1.0 + 1.0 * (2 * expId - adExpNum) / adExpNum * param.getDiffTargetCpaCoef)
    } else {
      targetCpaOrg
    }
    val idx = rand.nextInt()
    val adTargetingList = Range(0, param.getTargetingNum).toList.map { i =>
      Int.box(expId match {
        case 2 =>
          if (i == idx) rand.nextInt(2) else -1
        case 3 =>
          if (i == idx || i == (idx + 1) % param.getTargetingNum) rand.nextInt(2) else -1
        case _ =>
          -1
      })
    }
    val budgetPower = rand.nextInt(param.getBudgetPowerMax - param.getBudgetPowerMin + 1) + param.getBudgetPowerMin
    val budget = math.pow(10, budgetPower)
    val adjustFactor = 1
    AdCurrentInfo.newBuilder()
      .setAdId(id)
      .addExpInfo(ExpInfo.newBuilder().setExpId(expId.toInt))
      .setFeatures(AdFeatures.newBuilder().addAllAdFeature(adFeatureList).addAllAdTargeting(adTargetingList)
        .setAdjustFactor(adjustFactor).setTargetCpa(targetCpa).setIsNewAd(true)
        .setTotalBudget(budget).setRemainBudget(budget))
      .setAdRunningStatus(AdCurrentInfo.AdRunningStatus.RUNNING)
      .build()
  }

  def initUserInfo(implicit sc: SparkContext, piBc: ParamInfoBc): RDD[UserCurrentInfo] = {
    val pi = piBc.value
    val partitionNum = (pi.getBaseParam.getUserCnt / pi.getBaseParam.getRecordCntPerPartition).toInt
    if (partitionNum > 0) {
      sc.parallelize(Range(0, partitionNum), partitionNum).flatMap { i =>
        val pi = piBc.value
        val rand = new Random(i + 10000 + pi.getBaseParam.getRandomSeed * partitionNum)
        Range(0, pi.getBaseParam.getRecordCntPerPartition).map { j =>
          val id = i.toLong * pi.getBaseParam.getRecordCntPerPartition + j
          val userFeatureList = Range(0, pi.getElementParam.getRealUserFeatureNum).toList.map { i =>
            //            Double.box(rand.nextDouble())
            val randValue = (rand.nextDouble() - 0.5) * 2.0
            Double.box(randValue)
          }
          val expId = id % getExpNum(pi)._1
          val userTargetingList = Range(0, pi.getElementParam.getTargetingNum).toList.map { i =>
            Int.box(rand.nextInt(2))
          }
          UserCurrentInfo.newBuilder()
            .setUserId(id)
            .setFeatures(UserFeatures.newBuilder().addAllUserFeature(userFeatureList).addAllUserTargeting(userTargetingList))
            .addExpInfo(ExpInfo.newBuilder().setExpId(expId.toInt))
            .build()
        }
      }
    } else {
      sc.emptyRDD[UserCurrentInfo]
    }
  }

  def initAdInfo(implicit sc: SparkContext, piBc: ParamInfoBc): RDD[AdCurrentInfo] = {
    val pi = piBc.value
    val totalAdCnt = pi.getBaseParam.getAdCnt
    val copyAdCnt = pi.getBaseParam.getCopyAdCnt
    val orgAdCnt = totalAdCnt - copyAdCnt
    val recCntPerPar = pi.getBaseParam.getRecordCntPerPartition
    val partitionNum = if (orgAdCnt < recCntPerPar) 1 else (orgAdCnt / recCntPerPar).toInt
    println("initAdInfo partitionNum:" + partitionNum)
    if (partitionNum > 0) {
      val adRdd = sc.parallelize(Range(0, partitionNum), partitionNum).flatMap { i =>
        implicit val pi = piBc.value
        val adRandomSeed = if (pi.getBaseParam.getAdRandomSeed != 0) {
          pi.getBaseParam.getAdRandomSeed
        } else {
          pi.getBaseParam.getRandomSeed
        }
        val rand = new Random(i + 20000 + adRandomSeed * partitionNum)
        Range(0, (orgAdCnt / partitionNum).toInt).map { j =>
          val id = i * (orgAdCnt / partitionNum).toInt + j
          getNewAdPattern(id, rand)
        }
      }
      if (copyAdCnt > 0) {
        val adCopyRdd = adRdd.flatMap { ad =>
          if (ad.getAdId < copyAdCnt) {
            implicit val pi = piBc.value
            val pairAdId = ad.getAdId + orgAdCnt
            Some(AdCurrentInfo.newBuilder(ad).setAdId(pairAdId).build())
          } else {
            None
          }
        }
        adRdd.union(adCopyRdd).repartition(partitionNum)
      } else {
        adRdd
      }
    } else {
      sc.emptyRDD[AdCurrentInfo]
    }
  }


  def readUserInfoFromCheckpoint(path: String)(implicit sc: SparkContext): RDD[UserCurrentInfo] = {
    Util.readCheckPoint(path).map(UserCurrentInfo.parseFrom)
  }

  def readAdInfoFromCheckpoint(path: String)(implicit sc: SparkContext): RDD[AdCurrentInfo] = {
    Util.readCheckPoint(path).map(AdCurrentInfo.parseFrom)
  }
}
