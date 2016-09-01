
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File

/**
  * Created by brendan on 8/31/16.
  */

object sentencecounter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sentencecounter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("conspiracy.txt")

    val lines = input.map((_,1))
    val total = lines.reduceByKey(_+_)
    val sorted = total.sortByKey(true)
    val o = sorted.collect()

    var s: String="Sentences : Count \n"
    o.foreach{case(sent,count)=>{
      s += sent + " : " + count + "\n"
    }}

    File("output.txt").writeAll(s)
  }
}

