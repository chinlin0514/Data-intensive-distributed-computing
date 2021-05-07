package ca.uwaterloo.cs451.a6;

import org.apache.log4j._
import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._
import scala.math.exp    // to use exp

class TrainConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)			
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output model", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
  verify()
}

object TrainSpamClassifier{
	val log = Logger.getLogger(getClass.getName)	

	def main(argv: Array[String]){
		val args = new TrainConf(argv)
		log.info("Input: " + args.input())
		log.info("Model: " + args.model())
		//log.info("text: " + args.text())
		//log.info("parquet: "+ args.parquet())
		val conf = new SparkConf().setAppName("TrainSpamClassifier")
		val sc = new SparkContext(conf)
		val outputDir = new Path(args.model())
    	FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    	val textFile = sc.textFile(args.input(), 1)	

		if(args.shuffle()){ // if using shuffle
			// prefix each data element with a random integer such that this integer is now the key Then do sort by key
			// 2.7. Generating Random Numbers from https://www.oreilly.com/library/view/scala-cookbook/9781449340292/ch02s08.html
			val textFile = sc.textFile(args.input(), 1)
				.map(line=>{
					val r = scala.util.Random
					(r.nextInt,line)})
				.sortByKey()
				.map(p => p._2)
		} 

		val trained = textFile.map(line =>{
  		// Parse input
  			val splitLine = line.split("\\s+")		// split with spaces => get Array[String]
  			val docid = splitLine(0)				// 0 : docID, 1 : spam, 2:-1 : features
  			val spam = splitLine(1)
  			var isSpam = 0
  			if (spam == "spam"){
  				isSpam = 1
  			}
  			val feature = splitLine.slice(2,10000000).map(num => num.toInt) //If we give 2nd parameter value in beyond it’s available index 
  											  			  //it just ignores that value and returns up-to available index only				  			
  			(0, (docid, isSpam, feature))
  			}).groupByKey(1)	//output: (Int, Iterable[(String, Int, Array[Int])])

		// w is the weight vector (make sure the variable is within scope)
		// need to make the map mutable
		//val w = scala.collection.mutable.Map[Int, Double]()
		val w = Map[Int, Double]()
		// Scores a document based on its list of features.
		//def spamminess(features: Array[Int]) : Double = {var score = 0d;features.foreach(f => if (w.contains(f)) score += w(f));score}
		def spamminess(features: Array[Int]) : Double = {
		  var score = 0d
		  features.foreach(f => if (w.contains(f)) score += w(f))
		  score
		}

		// This is the main learner:
		val delta = 0.002

		// For each instance...
		trained.flatMap(line=>{
			//          docid, Spam, features
			line._2.foreach(l=>{		//line._2: (String, Int, Array[Int])
				val docid = l._1	
				val isSpam = l._2   	// label
				val features = l._3.toArray	// feature vector of the training instance
				// Update the weights as follows:
				// Update rule B <- B + delta(isSpam(p) - prob)
				val score = spamminess(features)		// spamminess takes array[int]
				val prob = 1.0 / (1 + exp(-score))
				features.foreach(f => {
				  if (w.contains(f)) {
				  	w(f) += (isSpam - prob) * delta
				  } else {
				  	w(f) = (isSpam - prob) * delta
				  }
				})
			})
			w
		}).saveAsTextFile(args.model())
	}
}

//trained.flatMap(line => {line._2.foreach(l => {val docid = l._1;val isSpam = l._2;val features = l._3;val score = spamminess(features);val prob = 1.0 / (1 + exp(-score));features.foreach(f => {if (w.contains(f)) {w(f) += (isSpam - prob) * delta}else {w(f) = (isSpam - prob) * delta}})})w})


