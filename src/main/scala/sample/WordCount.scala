package sample

import org.apache.spark.{SparkConf, SparkContext}

object BasicWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BasicWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("README.md")
    val words = textFile.flatMap(_.split(" "))
    val wordsCounts = words.map((_, 1)).reduceByKey((a, b) => a + b)

    wordsCounts.saveAsTextFile("./wordcounts")
  }
}
