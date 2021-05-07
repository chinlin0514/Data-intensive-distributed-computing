package ca.uwaterloo.cs451.a5;

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)						
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  verify()
}

object Q2{
	val log = Logger.getLogger(getClass.getName)		
	def main(argv: Array[String]){
		val args = new Q2Conf(argv)
		log.info("Input: " + args.input())
		log.info("date: " + args.date())
		log.info("text: " + args.text())
		log.info("parquet: "+ args.parquet())

		val l_shipdate = args.date()
		val conf = new SparkConf().setAppName("Q2")
		val sc = new SparkContext(conf)

		if(args.text()){			// Text
			val lineItem = sc.textFile(args.input() + "/lineitem.tbl")
			val lineitemTuple = lineItem.map(line => {			//Array[(String, String)]
				val l_orderkey = line.split("\\|")(0)
				val l_shipdate = line.split("\\|")(10)
				(l_orderkey, l_shipdate)}).filter{case (_, v) => v == l_shipdate}

			val order = sc.textFile(args.input() + "/orders.tbl")
			val orderTuple = order.map(line => {				//Array[(String, String)]
				val o_orderkey = line.split("\\|")(0) 
				val o_clerk = line.split("\\|")(6)
				(o_orderkey,o_clerk)})

			// cogroup
			val group = orderTuple
				.cogroup(lineitemTuple)
				.filter(x=>{x._2._2.size != 0})
				.sortByKey()
				.take(20)
				.map{x => 
					val a = x._2._1.mkString("")
					val b = x._2._2.mkString("")
					(a,b)}
					.foreach(println)
			//return (o_clerk,o_orderkey)
		} 						
		else{				// Parquet
			val sparkSession = SparkSession.builder.getOrCreate

			val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")	//val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
			val lineitemRDD = lineitemDF.rdd
			val orderDF = sparkSession.read.parquet(args.input() + "/orders")
			val orderRDD = orderDF.rdd
			val lineitemTuple = lineitemRDD.map(line => 			 //Array[(Int, String)]
				(line.getInt(0), line.getString(10))).filter(_._2 == l_shipdate) 
			val orderTuple = orderRDD.map(line => (line.getInt(0), line.getString(6)))		//Array[(Int, String)]
			val group = orderTuple.cogroup(lineitemTuple)
				.filter(x =>{x._2._2.size != 0})
				.sortByKey().take(20)
				.map{x => 
					val a = x._2._1.mkString("")
					val b = x._2._2.mkString("") 
					(a,b)}
					.foreach(println)
		}
	}
}