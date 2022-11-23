package ams.exp.market_simulation.module.display

import java.util.Random

import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import ams.exp.market_simulation.MarketSimulationProtos.ModelInfo.LRModel
import ams.exp.market_simulation.MarketSimulationProtos.{AdCurrentInfo, ModelInfo, RequestInfo, UserCurrentInfo}
import ams.exp.market_simulation.module.effect.EffectSimulation
import ams.exp.market_simulation.module.util.Util
import ams.exp.market_simulation.module.util.Util.ParamInfoBc
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object PredictModelSimulation {

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private type TrainData = (Long, Long, Int, Double)

  def getPrdCvrByLrModel(user: UserCurrentInfo, ad: AdCurrentInfo, lrModel: LRModel)(implicit pi: ParamInfo): Double = {
    val param = pi.getPredictModelParam
    lrModel.getIntercept +
      lrModel.getUserCoefList
        .zip(user.getFeatures.getUserFeatureList.take(param.getRankModelFeatureNum))
        .zip(ad.getFeatures.getAdFeatureList.take(param.getRankModelFeatureNum))
        .map {
          case ((coef, userFeature), adFeature) =>
            coef * userFeature * adFeature
        }.sum
  }

  def getPrdCvrByColdStart(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, rankStage: Int)(implicit pi: ParamInfo): Double = {
    EffectSimulation.getRealCvr(user, ad) * getPrdCvrBias(user, ad) * (1.0 + getEpsilon(reqId, user, ad, rankStage + 1))
  }

  def getAdjustPrdCvr(pCvr: Double, user: UserCurrentInfo, ad: AdCurrentInfo)(implicit pi: ParamInfo): Double = {
    val weightList = pi.getPredictModelParam.getAdjustPrdCvrWeightList.split(',').map(_.toDouble)
    if (ad.getMetrics.getConversion >= pi.getDisplayParam.getNewAdConvThreshold && user.getMetrics.getImpression > 0) {
      weightList(0) * pCvr + weightList(1) * ad.getMetrics.getConversion / ad.getMetrics.getImpression
    } else {
      pCvr
    }
  }

  def getRankPrdCvr(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, modelInfo: ModelInfo)(implicit pi: ParamInfo): Double = {
    val param = pi.getPredictModelParam
    val prdCvr = if (param.getUseRankLrModel && modelInfo.hasRankModel) {
      val rankModel = modelInfo.getRankModel
      getPrdCvrByLrModel(user, ad, rankModel)
    } else {
      getPrdCvrByColdStart(reqId, user, ad, 1)
    }
    getAdjustPrdCvr(prdCvr, user, ad)
  }

  def getRerankPrdCvr(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, modelInfo: ModelInfo)(implicit pi: ParamInfo): Double = {
    val param = pi.getPredictModelParam
    val prdCvr = if (param.getUseRerankLrModel && modelInfo.hasRerankModel) {
      val rerankModel = modelInfo.getRerankModel
      getPrdCvrByLrModel(user, ad, rerankModel)
    } else {
      getPrdCvrByColdStart(reqId, user, ad, 2)
    }
    getAdjustPrdCvr(prdCvr, user, ad)
  }

  def getPrdCvrBias(user: UserCurrentInfo, ad: AdCurrentInfo)(implicit pi: ParamInfo): Double = {
    val param = pi.getPredictModelParam
    val temp = if ((pi.getDisplayParam.getUseSplitUpliftTest || pi.getDisplayParam.getUseAdSupport) &&
      ad.getExpInfo(0).getExpId < 2 &&
      ad.getMetrics.getConversion < pi.getDisplayParam.getNewAdConvThreshold) pi.getPredictModelParam.getPrdCvrAdjustCoef else 1.0
    val temp2 = if (DisplaySimulation.getExpCoef(user, ad) > 0) {
      1.0 + param.getCvrBiasLift
    } else {
      1.0
    }
    (param.getBaseCvrBias + 1.0) * temp * temp2
  }

  def getEpsilon(reqId: Long, user: UserCurrentInfo, ad: AdCurrentInfo, randNum: Int)(implicit pi: ParamInfo): Double = {
    val param1 = pi.getBaseParam
    val param2 = pi.getPredictModelParam
    val rand = new Random(reqId + user.getUserId + ad.getAdId + param1.getRandomSeed *
      (param1.getReqCnt + param1.getAdCnt + param1.getReqCnt))
    val randDouble = Range(0, randNum).map(i => rand.nextDouble()).last
    randDouble * (param2.getEpsilonMax - param2.getEpsilonMin) + param2.getEpsilonMin
  }

  def initModelInfo: ModelInfo = {
    ModelInfo.newBuilder().build()
  }

  def readModelInfoFromCheckpoint(path: String)(implicit sc: SparkContext): ModelInfo = {
    Util.readCheckPoint(path).map(ModelInfo.parseFrom).take(1).headOption.getOrElse(ModelInfo.newBuilder().build())
  }

  def getTrainFeatureList(user: UserCurrentInfo, ad: AdCurrentInfo, modelFeatureNum: Int)(implicit pi: ParamInfo): List[Double] = {
    if (pi.getPredictModelParam.getUseInteractions) {
      user.getFeatures.getUserFeatureList.take(modelFeatureNum)
        .zip(ad.getFeatures.getAdFeatureList.take(modelFeatureNum))
        .map { case (userFeature, adFeature) =>
          Double2double(userFeature * adFeature) * pi.getElementParam.getTargetCpaConst / ad.getFeatures.getTargetCpa
        }.toList
    } else {
      (user.getFeatures.getUserFeatureList.take(modelFeatureNum) ++ ad.getFeatures.getAdFeatureList.take(modelFeatureNum)).map { feature =>
        Double2double(feature) * pi.getElementParam.getTargetCpaConst / ad.getFeatures.getTargetCpa
      }.toList
    }
  }

  def updateModel(fullEffectInfo: RDD[RequestInfo])(implicit piBc: ParamInfoBc): ModelInfo = {
    val pi = piBc.value
    val model = ModelInfo.newBuilder()
    if (pi.getPredictModelParam.getUseRankLrModel && pi.getDisplayParam.getUseTwoStepRanking) {
      val rankData = fullEffectInfo.flatMap { effect =>
        implicit val pi = piBc.value
        val featureList = getTrainFeatureList(effect.getUserCurrentInfo,
          effect.getRankingAdInfo(0),
          pi.getPredictModelParam.getRankModelFeatureNum)
        val conv = effect.getMetrics.getConversion
        featureList.zipWithIndex.map {
          case (feature, idx) =>
            (effect.getRequestId, conv, idx, feature)
        }
      }
      val featureNum = if (pi.getPredictModelParam.getUseInteractions) {
        pi.getPredictModelParam.getRankModelFeatureNum
      } else {
        2 * pi.getPredictModelParam.getRankModelFeatureNum
      }
      val rankLrModel = trainLrModel(rankData, featureNum)
      model.setRankModel(rankLrModel)
    }
    if (pi.getPredictModelParam.getUseRerankLrModel) {
      val rerankData = fullEffectInfo.flatMap { effect =>
        implicit val pi = piBc.value
        val featureList = getTrainFeatureList(effect.getUserCurrentInfo,
          effect.getRerankingAdInfo(0),
          pi.getPredictModelParam.getRerankModelFeatureNum)
        val conv = effect.getMetrics.getConversion
        featureList.zipWithIndex.map {
          case (feature, idx) =>
            (effect.getRequestId, conv, idx, feature)
        }
      }
      val featureNum = if (pi.getPredictModelParam.getUseInteractions) {
        pi.getPredictModelParam.getRerankModelFeatureNum
      } else {
        2 * pi.getPredictModelParam.getRerankModelFeatureNum
      }
      val rerankLrModel = trainLrModel(rerankData, featureNum)
      model.setRerankModel(rerankLrModel)
    }
    model.build()
  }

  def trainLrModel(data: RDD[TrainData], featureNum: Int): LRModel = {
    val spark: SparkSession = SparkSession.builder.getOrCreate
    val df0 = spark.createDataFrame(data).toDF
    val df = df0.groupBy("_1", "_2").pivot("_3").agg(Map("_4" -> "avg"))
    df.show(10)
    val featureNameArray = Range(0, featureNum).map(_.toString).toArray
    val labelName = "_2"
    val features = new VectorAssembler()
      .setInputCols(featureNameArray)
      .setOutputCol("features")
    val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0)
      .setSolver("normal")
      .setLabelCol(labelName)
    val pipeline = new Pipeline().setStages(Array(features, lr))
    val model = pipeline.fit(df)
    val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]
    val intercept = linRegModel.intercept
    val coefficients = linRegModel.coefficients.toArray.map(Double.box).toList
    val lrModel = LRModel.newBuilder()
    lrModel.setIntercept(intercept)
    lrModel.addAllUserCoef(coefficients.take(featureNum))
    lrModel.build()
  }
}
