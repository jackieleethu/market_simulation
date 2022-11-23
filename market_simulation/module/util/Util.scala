package ams.exp.market_simulation.module.util

import java.io._

import ams.exp.market_simulation.MarketSimulationConfigProtos.ParamInfo
import com.gdt.recordio.mapreduce.{RecordIOBytesInputFormat, RecordIOBytesRawOutputFormat}
import com.google.protobuf.TextFormat
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.util.Try

object Util extends Serializable {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  type ParamInfoBc = Broadcast[ParamInfo]

  def initParamInfo = {
    val str = {
      val localPath = "/market_simulation_config"
      //    val in = new FileInputStream(localPath)
      val in: InputStream = this.getClass.getResourceAsStream(localPath)
      val bufferedReader = new BufferedReader(new InputStreamReader(in))
      def readLines = Stream.cons(bufferedReader.readLine, Stream.continually(bufferedReader.readLine))
      readLines.takeWhile(_ != null).mkString("\n")
    }
    println(str)
    val builder = ParamInfo.newBuilder()
    TextFormat.merge(str, builder)
    val paramInfo = builder.build()
    //    println("paramInfo:" + paramInfo)
    paramInfo
  }

  def printRddDetail[A: ClassTag](rddName: String, rdd: RDD[A])(f: A => Long)(implicit printDetail: Boolean) = {
    if (printDetail) {
      println(s"rddName:$rddName")
      rdd.mapPartitions(iter => iter.toList.sortBy(f).headOption.toIterator).collect().foreach(println)
      rdd.sortBy(f).take(10).foreach(println)
      println(s"rddName.count(): ${rdd.count()}")
    }
  }

  def removeHdfsPath(hdfsPath: String)(implicit sc: SparkContext) = {
    val path = new Path(hdfsPath)
    val conf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(conf)
    if (hdfs.exists(path)) hdfs.delete(path, true)
  }

  def saveCheckPoint(checkpoint: RDD[Array[Byte]], checkpointPath: String, outputPartitionNum: Int) = {
    checkpoint.cache().map(bytes => (NullWritable.get(), bytes)).repartition(outputPartitionNum)
      .saveAsNewAPIHadoopFile[RecordIOBytesRawOutputFormat](checkpointPath)

  }

  def readRecordIO(path: String)(implicit sc: SparkContext) = {
    sc.newAPIHadoopFile[NullWritable, Array[Byte], RecordIOBytesInputFormat](path)
      .map(_._2)
  }

  def readCheckPoint(checkpointPath: String)(implicit sc: SparkContext): RDD[Array[Byte]] = {
    getLatestPathAndTime(checkpointPath).map {
      case (latestPath, latestTime) =>
        log.info(s"latestPath: $latestPath, latestTime: $latestTime")
        sc.newAPIHadoopFile[NullWritable, Array[Byte], RecordIOBytesInputFormat](latestPath)
          .map(_._2)
    }.getOrElse(sc.emptyRDD[Array[Byte]])
  }

  def writeToHdfs(filePath: String, content: String)(implicit sc: SparkContext) = {
    val p = new Path(filePath)
    val fs = p.getFileSystem(sc.hadoopConfiguration)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(p, true)))
    writer.write(content)
    writer.close()
  }

  def readFromHdfs(filePath: String)(implicit sc: SparkContext) = {
    Try {
      val triggerPath = new Path(filePath)
      val hdfs = triggerPath.getFileSystem(sc.hadoopConfiguration)
      val in = hdfs.open(triggerPath)
      val bufferedReader = new BufferedReader(new InputStreamReader(in))
      def readLines = Stream.cons(bufferedReader.readLine, Stream.continually(bufferedReader.readLine))
      readLines.takeWhile(_ != null).mkString("\n")
    }.toOption
  }

  def getLatestPathAndTime(inputPath: String): Option[(String, String)] = {
    val path = new Path(inputPath)
    val conf = new Configuration()
    val fs = FileSystem.get(path.toUri, conf)
    val latestPath = fs.listStatus(path).map { p =>
      (p.getPath.toString, p.getPath.getName)
    }.sortBy(_._1).lastOption
    fs.close()
    latestPath
  }

  def getBatchTimeString(time: Time): String = {
    val timeMill = time.milliseconds
    DateFormatUtils.format(timeMill, "yyyyMMddHHmm")
  }

  // 利用foldLeft构造的重试方法，用于各种重试操作(传入T类型参数用于记录跟踪中间变量）
  def repeatedTry[T](maxTryTime: Int, initValue: T)(f: (Boolean, T) => (Boolean, T)): (Boolean, T) = {
    Range(0, maxTryTime).toList.foldLeft((false, initValue)) {
      case ((success, value), _) =>
        if (success) {
          (true, value)
        } else {
          f.apply(success, value)
        }
    }
  }

  // 利用foldLeft构造的重试方法，用于各种重试操作(传入T类型参数用于记录跟踪中间变量）
  def repeatedTry[T](maxTryTime: Int)(f: Boolean => Boolean): Boolean = {
    Range(0, maxTryTime).toList.foldLeft(false) {
      case (success, _) =>
        if (success) {
          true
        } else {
          f.apply(success)
        }
    }
  }

  def checkFileReady(success: Boolean, waitingTime: Long, hdfsPath: String): (Boolean, Long) = {
    val sc = SparkContext.getOrCreate()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsPath), sc.hadoopConfiguration)
    val path = new Path(hdfsPath)
    Thread.sleep(10 * 1000L)
    val newSuccess = hdfs.exists(path)
    val newWaitingTime = waitingTime + 10 * 1000L
    (newSuccess, newWaitingTime)
  }

  def checkUntilReady(hdfsPath: String, batchDuration: Int) = {
    // 每10s重试一次，最大重试次数为batchDuration*6
    val maxTryTime = batchDuration * 6
    repeatedTry(maxTryTime, 0L) { (success, waitingTime) =>
      checkFileReady(success, waitingTime, hdfsPath)
    }
  }

  def readLastReqIdFromCheckpoint(path: String)(implicit sc: SparkContext) = {
    readFromHdfs(path).getOrElse("0").toLong
  }

  def saveReqIdToCheckpoint(reqId: Long, path: String)(implicit sc: SparkContext) = {
    writeToHdfs(path, reqId.toString)
  }
}
