package ca.uwaterloo.cs451.a5;

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession


class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)						
  val input = opt[String](descr = "input path", required = true)
  //val output = opt[String](descr = "output path", required = true)
  val date = opt[String](descr = "l_shipdate", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object Q1{
	val log = Logger.getLogger(getClass.getName)		
	def main(argv: Array[String]){
		val args = new Q1Conf(argv)
		log.info("Input: " + args.input())
		//log.info("Output: " + args.output())
		//log.info("Number of reducers: " + args.reducers())
		//log.info("Use in-mapper combining: " + args.imc())
		//log.info("Use in-mapper histogram: " + args.histogram())
		log.info("date: " + args.date())
		log.info("text: " + args.text())
		log.info("parquet: "+ args.parquet())

		val l_shipdate = args.date()
		val conf = new SparkConf().setAppName("Q1")
		val sc = new SparkContext(conf)


		if(args.text()){			// Text
			val lineItem = sc.textFile(args.input() + "/lineitem.tbl")		// val lineItem = sc.textFile("TPC-H-0.1-TXT/lineitem.tbl")
			//lineItem.filter(line => line.contains("date")).count() -> wrong, could be on different columns
			val count = lineItem.map(line => line.split("\\|")(10)).filter(_ == l_shipdate).count()
			println("ANSWER=" + count)
		} 						
		else{				// Parquet
			val sparkSession = SparkSession.builder.getOrCreate

			val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")	//val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
			val lineitemRDD = lineitemDF.rdd
			val count = lineitemRDD.map(line => line.getString(10)).filter(_ == l_shipdate).count()
			println("ANSWER=" + count)
		}
	}
}
