package ca.uwaterloo.cs451.a6;

import org.apache.log4j._
import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._
import scala.math.exp    // to use exp

class EnsembleConf(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, output, model, method)			
  	val input = opt[String](descr = "input path", required = true)
  	val model = opt[String](descr = "input model", required = true)
  	val output = opt[String](descr = "output path", required = true)
  	val method = opt[String](descr = "Method", required = true) 	//average or vote
  	verify()
}

object ApplyEnsembleSpamClassifier{
	val log = Logger.getLogger(getClass.getName)	

	def main(argv: Array[String]){
		val args = new EnsembleConf(argv)
		log.info("Input: " + args.input())
		log.info("Model: " + args.model())
		log.info("Output: " + args.output())
		log.info("Method: " + args.method())

		val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
		val sc = new SparkContext(conf)
		val outputDir = new Path(args.output())
    	FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    	val testFile = sc.textFile(args.input(), 1)
    	val meth = args.method()
		val model_group_x = sc.textFile(args.model() + "/part-00000")
		val model_group_y = sc.textFile(args.model() + "/part-00001")
		val model_britney = sc.textFile(args.model() + "/part-00002")

		// GET broadcast weight from three models
    	val w_x = sc.broadcast(model_group_x   			//Map[Int,Double]
    		.map(line => 
    			{val split = line.split(',')
    			val featureID = split(0).slice(1,split(0).length())
    			val weight = split(1).slice(0,split(1).length()-1)
    			(featureID.toInt,weight.toDouble)})
    		.collectAsMap())

    	val w_y = sc.broadcast(model_group_y   			//Map[Int,Double]
    		.map(line => 
    			{val split = line.split(',')
    			val featureID = split(0).slice(1,split(0).length())
    			val weight = split(1).slice(0,split(1).length()-1)
    			(featureID.toInt,weight.toDouble)})
    		.collectAsMap())

    	val w_b = sc.broadcast(model_britney  			//Map[Int,Double]
    		.map(line => 
    			{val split = line.split(',')
    			val featureID = split(0).slice(1,split(0).length())
    			val weight = split(1).slice(0,split(1).length()-1)
    			(featureID.toInt,weight.toDouble)})
    		.collectAsMap())

    	//weight is Map[Int,Double]
		def spamminess(features: Array[Int], weight: scala.collection.Map[Int, Double]): Double = {
		  var score = 0d
		  features.foreach(f => if (weight.contains(f)) score += weight(f))
		  score
		}

		testFile.map(line => 
			{val splitLine = line.split("\\s+")
			val docid = splitLine(0)
			val spam = splitLine(1)
			val feature = splitLine.slice(2,10000000).map(num => num.toInt)
			val score_x = spamminess(feature, w_x.value)
			val score_y = spamminess(feature, w_y.value)
			val score_britney= spamminess(feature, w_b.value)
			var score = 0.toDouble
			if(meth == "average"){
				score = (score_x + score_y + score_britney)/3
			}
			if(meth == "vote"){
				var num_spam = 0
				var num_ham = 0
				if(score_x>0){
					num_spam += 1
				} else num_ham += 1
				if(score_y>0){
					num_spam += 1
				} else num_ham += 1
				if(score_britney>0){
					num_spam += 1
				} else num_ham += 1
				score = num_spam-num_ham	//# spam - # ham
			}	
			var predicted = ""
			if(meth == "average"){
				if (score > 0) predicted = "spam" else predicted = "ham"}
			if(meth == "vote"){
				if (score > 0) predicted = "spam" else predicted = "ham"}
			(docid, spam, score, predicted)
		}).saveAsTextFile(args.output())

		}
	}
