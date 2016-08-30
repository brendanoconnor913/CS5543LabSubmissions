/**
  * Created by brendan on 8/30/16.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File

object wordcount {
  def main(args: Array[String]): Unit = {
    // configure settings for spark and file
    val conf = new SparkConf().setAppName("FileWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("conspiracy.txt")

    // map each word to a 2 tuple then reduce and sort tuples
    val wc = input.flatMap(_.split("\\s+")).map(_.toLowerCase()).map((_,1)).cache()
    val output = wc.reduceByKey((l1,l2) => l1+l2)
    val ordered = output.sortByKey(true)
    val o = ordered.collect()

    // create string of words and their count for output
    var s:String = "Words:Count \n"
    o.foreach{case(word,count) => {
      s += word+ " : " + count + "\n"
    }}

    File("outputtext").writeAll(s)
  }
}