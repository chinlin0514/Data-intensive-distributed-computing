package ca.uwaterloo.cs451.a5;

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)						
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  verify()
}

object Q3{
	val log = Logger.getLogger(getClass.getName)		
	def main(argv: Array[String]){
		val args = new Q3Conf(argv)
		log.info("Input: " + args.input())
		log.info("date: " + args.date())
		log.info("text: " + args.text())
		log.info("parquet: "+ args.parquet())

		val l_shipdate = args.date()
		val conf = new SparkConf().setAppName("Q3")
		val sc = new SparkContext(conf)

		//create a Map structure, pass this map to the working node, search in the map
		// Use collectasmap instead of collect
		if(args.text()){			// Text
			val lineItem = sc.textFile(args.input() + "/lineitem.tbl")
			val part = sc.textFile(args.input() + "/part.tbl")	
			val supplier = sc.textFile(args.input() + "/supplier.tbl")

			val part_tuple = part.map(line=> { 			//Map[String,String]
				val partkey = line.split("\\|")(0)
				val name = line.split("\\|")(1)
				(partkey, name)}).collectAsMap()		
			val part_map_b = sc.broadcast(part_tuple)

			val supp_tuple = supplier.map(line=> {			//Map[String,String]	
				val suppkey = line.split("\\|")(0)
				val s_name = line.split("\\|")(1) 
				(suppkey, s_name)}).collectAsMap()	
			val supp_map_b = sc.broadcast(supp_tuple)

			val keys = lineItem.filter(line => {
				line.split("\\|")(10) == l_shipdate}).map(line => {
					val orderkey = line.split("\\|")(0).toInt
					val partkey = part_map_b.value(line.split("\\|")(1))
					val suppkey = supp_map_b.value(line.split("\\|")(2))
					(orderkey, partkey, suppkey)}).sortBy(_._1).take(20).foreach(println)		// not sure if need sort or not

		} 						
		else{				// Parquet
			val sparkSession = SparkSession.builder.getOrCreate

			val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")	//val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
			val lineitemRDD = lineitemDF.rdd
			val partDF = sparkSession.read.parquet(args.input() + "/part")
			val partRDD = partDF.rdd
			val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
			val supplierRDD = supplierDF.rdd
			val part_tuple = partRDD.map(line => (line(0), line(1))).collectAsMap()  // (any, any) No implicit Ordering defined for Any.
			val part_map_b = sc.broadcast(part_tuple)
			val supp_tuple = supplierRDD.map(line => (line(0), line(1))).collectAsMap()		//(any, any)
			val supp_map_b = sc.broadcast(supp_tuple)
			val keys = lineitemRDD
				.filter(line => line.getString(10) == l_shipdate)
				.map(line => {
					val orderkey = line.getInt(0)			// need order for sortBy, so specify Int 
					val partkey = part_map_b.value(line(1))
					val suppkey = supp_map_b.value(line(2))
					(orderkey, partkey, suppkey)}).sortBy(_._1).take(20).foreach(println)
		}
	}
}
