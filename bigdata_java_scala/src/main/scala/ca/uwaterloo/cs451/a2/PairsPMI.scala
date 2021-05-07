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

package ca.uwaterloo.cs451.a2;

import io.bespin.scala.util.Tokenizer

import scala.collection.mutable.ListBuffer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class PMIPairsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold for pmi occurrence", required = false, default = Some(0))
  verify()
}

class KeyPartitioner3(numOfReducers : Int) extends Partitioner 
{
  def numPartitions : Int = numOfReducers
  def getPartition(matchKey: Any) : Int =  matchKey match
  {
    case null => 0
    case (leftKey, rightKey) => (leftKey.hashCode & Integer.MAX_VALUE) % numOfReducers
    case _ => 0
  }
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PMIPairsConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)
    var total = 0.0;

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val threshold = args.threshold()
    val totalNumberOfLines = textFile.count()
    val wordHashMap = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take( if(tokens.length < 40) tokens.length else 40 ).distinct 
        else List()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap()
    val wordTotals = sc.broadcast(wordHashMap)
    
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val stripes = ListBuffer[(String, String)]()
        val words = tokens.take(if(tokens.length < 40) tokens.length else 40).distinct
        val totalWords = words.length

        if(totalWords > 1) {
          for( i <- 0 to totalWords - 1) {
            for( j <- i + 1 to totalWords - 1) {
              var stripe1 : (String, String) = (words(i), words(j))
              var stripe2 : (String, String) = (words(j), words(i))
              stripes += stripe1
              stripes += stripe2
            }
          }
          stripes.toList
        } else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .filter((kv) => kv._2 >= threshold)
      .repartitionAndSortWithinPartitions(new KeyPartitioner3(args.reducers()))
      .map(keyValue => 
        (
          keyValue._1,
          (
            Math.log10((keyValue._2.toFloat * totalNumberOfLines.toFloat) / ( wordTotals.value(keyValue._1._1).toFloat * wordTotals.value(keyValue._1._2).toFloat)),
            keyValue._2.toFloat
          )
        ))
    counts.saveAsTextFile(args.output())
  }
}

