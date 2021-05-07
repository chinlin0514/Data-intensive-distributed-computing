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

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[String](descr = "number of executors", required = false, default = Some("1"))
  val executorCores = opt[String](descr = "number of cores", required = false, default = Some("1"))
  verify()
}

class KeyPartitioner(numPar : Int) extends Partitioner 
{
  def numPartitions : Int = numPar
  def getPartition(matchKey: Any) : Int =  matchKey match
  {
    case null => 0
    case (leftKey, rightKey) => (leftKey.hashCode & Integer.MAX_VALUE) % numPar
    case _ => 0
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    //log.info("Number of executors: " + args.numExecutors())
    //log.info("Number of cores: " + args.executorCores())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency")
    //conf.set("spark.executor.instances", args.numExecutors())
    //conf.set("spark.executor.cores", args.executorCores())


    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val textFile = sc.textFile(args.input())

    // apply flatmap to textfile, use tokens.sliding(2) to find bigram
    var total = 0.0;
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        // apply mkString to add space in the flatmap
        if (tokens.length > 1) tokens.sliding(2).flatMap(p => List( p.head + " *", p.mkString(" ") )) else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .repartitionAndSortWithinPartitions(new KeyPartitioner(args.reducers()))
      .map(key => 
          {
            val leftKey = key._1.split(" ")(0)
            val rightKey = key._1.split(" ")(1)
            if (rightKey == "*") {
              total = key._2
              ( (leftKey, rightKey), key._2 )
            }
            else ((leftKey, rightKey), key._2 / total )
          }
        )
    counts.saveAsTextFile(args.output())
  }
}
