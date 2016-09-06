import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by brendan on 9/6/16.
  */

object textmanip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("BillWordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("TrumpRNCSpeech.txt")
    val process = input.flatMap(_.split(" ")).map(_.toLowerCase()).cache()
    val pairs = process.map((_,1))
    val count = pairs.reduceByKey(_+_)
    val ordered = count.map(_.swap).sortByKey(false).map(_.swap)
    ordered.saveAsTextFile("Trump") // trump word count

    val input2 = sc.textFile("ClintonDNCSpeech.txt")
    val process2 = input2.flatMap(_.split(" ")).map(_.toLowerCase()).cache()
    val pairs2 = process2.map((_,1))
    val count2 = pairs2.reduceByKey(_+_)
    val ordered2 = count2.map(_.swap).sortByKey(false).map(_.swap)
    ordered2.saveAsTextFile("Clinton") // clinton word count

    val trumpWords = process.subtract(process2)
    val tpairs = trumpWords.map((_,1))
    val tcount = tpairs.reduceByKey(_+_)
    val tordered = tcount.map(_.swap).sortByKey(false).map(_.swap)
    tordered.saveAsTextFile("TrumpWords") // words trump used that cliton didn't

    val clintonWords = process2.subtract(process)
    val cpairs = clintonWords.map((_,1)).cache()
    val ccount = cpairs.reduceByKey(_+_)
    val cordered = ccount.map(_.swap).sortByKey(false).map(_.swap)
    cordered.saveAsTextFile("ClintonWords") // words clinton used that trump didn't
  }
}