package sample

import org.apache.spark.{SparkConf, SparkContext}

object BasicFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BasicFilter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("README.md")
    val lines = textFile.filter(_.contains("Scala"))
    lines.saveAsTextFile("./filter")
  }
}
