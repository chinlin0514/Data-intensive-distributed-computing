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
import org.apache.spark.Partitioner
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class CBRFSConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object CombineMaps {
  type Counts = Map[String,Int]	
  def combine(x: Counts, y: Counts): Counts = {
    val x0 = x.withDefaultValue(0)
    val y0 = y.withDefaultValue(0)
    val keys = x.keys.toSet.union(y.keys.toSet)
    keys.map{ k => (k -> (x0(k) + y0(k))) }.toMap
  }
}

class KeyPartitioner_2(numOfReducers : Int) extends Partitioner 
{
  def numPartitions : Int = numOfReducers
  def getPartition(matchKey: Any) : Int =  matchKey match
  {
    case null => 0
    case (leftKey, rightKey) => (leftKey.hashCode & Integer.MAX_VALUE) % numOfReducers
    case _ => 0
  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new CBRFSConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => ( p.head, Map(p.tail.head -> 1) )).toList else List()
      })
      .reduceByKey((stripe1, stripe2) => {
        stripe1 ++ stripe2.map{ case (k, v) => k -> (v + stripe1.getOrElse(k, 0)) }
      })
      .repartitionAndSortWithinPartitions(new KeyPartitioner_2(args.reducers()))
      .mapValues(value => {
        var sum = 0;
        for ((secondkey, total) <- value) {
          sum += total;
        }
        value.mapValues((x) => x.toFloat / sum.toFloat)
      })
      .flatMap(p => List("(" + p._1 + ", Map{" + (p._2 mkString ", ") + "})")
      )
    counts.saveAsTextFile(args.output())
  }
}

