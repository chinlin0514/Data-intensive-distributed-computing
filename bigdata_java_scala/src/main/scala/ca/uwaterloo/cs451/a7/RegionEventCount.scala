/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    // get coordinates
    val goldman : List[List[Double]] = List(List(-74.0141012, 40.7152191), List(-74.013777, 40.7152275),List(-74.0141027, 40.7138745),List(-74.0144185, 40.7140753))
    val citigroup : List[List[Double]] = List(List(-74.011869, 40.7217236), List(-74.009867, 40.721493),List(-74.010140,40.720053),List(-74.012083, 40.720267))

    // check if points in box 
    def inBox(dropoff: String, long: Double, lat: Double): Boolean = {
      var inLong = false
      var inLat = false
      var inBoundBox = false 
      if (dropoff == "goldman"){
        if ((long <= goldman(1)(0)) && (long >= goldman(3)(0)))
          inLong = true
        if ((lat <= goldman(1)(1) && (lat >= goldman(2)(1))))
          inLat = true
      }
      else if (dropoff == "citigroup"){
        if ((long <= citigroup(1)(0)) && (long >= citigroup(3)(0)))
          inLong = true
        if ((lat <= citigroup(0)(1) && (lat >= citigroup(2)(1))))
          inLat = true
      }
      if (inLong && inLat){
        inBoundBox = true
      }
      return inBoundBox 
    }

    val wc = stream.map(_.split(","))
      .map(tuple => {
        val taxiType = tuple(0)
        var x = 0.toDouble
        var y = 0.toDouble
        
        if (taxiType == "yellow"){        
          x = tuple(10).toDouble
          y = tuple(11).toDouble
        }
        else{                  
          x = tuple(8).toDouble
          y = tuple(9).toDouble
        }

        if (inBox("goldman", x, y) == true){
          ("goldman", 1)
        }
        else if (inBox("citigroup", x, y) == true){
          ("citigroup", 1)
        }
        else{
          ("wrong", 1)      // add this because warning: item is not value of any
        }
      })
      .filter(item => item._1 == "goldman" || item._1 == "citigroup")      //https://www.geeksforgeeks.org/scala-map-filter-method-with-example/
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

