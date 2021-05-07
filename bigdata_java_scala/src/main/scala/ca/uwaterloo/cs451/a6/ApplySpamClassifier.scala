package ca.uwaterloo.cs451.a6;

import org.apache.log4j._
import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._
import scala.math.exp    // to use exp

//Use the output (part-00000) of trainspamClassifier as trained model => (feature, weight)

class ApplyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)			
  val input = opt[String](descr = "input path", required = true)
  //val shuffle = opt[Boolean](descr = "shuffle", required = false)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "input model", required = true)
  verify()
}

object ApplySpamClassifier{
	val log = Logger.getLogger(getClass.getName)	

	def main(argv: Array[String]){
		val args = new ApplyConf(argv)
		log.info("Input: " + args.input())
		log.info("Model: " + args.model())
		log.info("Output: " + args.output())


		val conf = new SparkConf().setAppName("ApplySpamClassifier")
		val sc = new SparkContext(conf)
		val outputDir = new Path(args.output())
    	FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    	//Read test and model files
    	val testFile = sc.textFile(args.input(), 1)	//val testFile = sc.textFile("spam.test.qrels.txt")
    	//val model = args.model() + "/part-00000"//--model : "cs451-bigdatateach-a6-model-group_x/part-00000"
    	val model = sc.textFile(args.model(), 1)

    	val w = sc.broadcast(model   			//Map[Int,Double]
    		.map(line => 
    			{val split = line.split(',')
    			val featureID = split(0).slice(1,split(0).length())
    			val weight = split(1).slice(0,split(1).length()-1)
    			(featureID.toInt,weight.toDouble)})
    		.collectAsMap())		// get pairs with unique keys and pairs with duplicate keys will be removed

		def spamminess(features: Array[Int]) : Double = {
		  var score = 0d
		  val wMap = w.value
		  features.foreach(f => if (wMap.contains(f)) score += wMap(f))
		  score
		}

		testFile.map(line => {
			val splitLine = line.split("\\s+")
			val docid = splitLine(0)
			val spam = splitLine(1)
			val feature = splitLine.slice(2,10000000).map(num => num.toInt)
			var isSpam = "spam"
			val score = spamminess(feature)
			if (score<0){isSpam = "ham"}
			(docid,spam,score,isSpam)
		}).saveAsTextFile(args.output())
    }
}

