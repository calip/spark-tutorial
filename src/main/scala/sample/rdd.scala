import org.apache.spark.{SparkConf, SparkContext}

case class Acc(sum: Double, count: Int) {
  def +(n: Int): Acc = {
    new Acc(this.sum + n, this.count + 1)
  }

  def ++(other: Acc): Acc = {
    new Acc(
      this.sum + other.sum,
      this.count + other.count
    )
  }
}

object RddPractice {
  def fruits(sc: SparkContext) =
    sc.parallelize(Array(
      ("Apple", 6), ("Orange", 1), ("Apple", 2),
      ("Orange", 5), ("PineApple", 1))
    )

  def aggregateByKey(sc: SparkContext) =
    fruits(sc).aggregateByKey(Acc(0.0, 0))(
      (acc, n) => acc + n,
      (a, b) => a ++ b
    ).mapValues(acc => acc.sum / acc.count).collect

  def combineByKey(sc: SparkContext) =
    fruits(sc).combineByKey(
      (n: Int) => Acc(n.toDouble, 1),
      (acc: Acc, n) => acc + n,
      (a: Acc, b: Acc) => a ++ b
    ).mapValues(acc => acc.sum / acc.count).collect

  def groupByKey(sc: SparkContext) =
    fruits(sc).groupByKey().collect

  def sortByKey(sc: SparkContext, ascending: Boolean = true) =
    fruits(sc).sortByKey(ascending).collect()

  def collectAsMap(sc: SparkContext) =
    fruits(sc).collectAsMap

  def persons(sc: SparkContext) =
    sc.parallelize(Seq(
      ("Adam", "San francisco"),
      ("Bob", "San francisco"),
      ("Taro", "Tokyo"),
      ("Charles", "New York")
    ))

  def cities(sc: SparkContext) =
    sc.parallelize(Seq(
      ("Tokyo", "Japan"),
      ("San francisco", "America"),
      ("Beijing", "China")
    ))
}

// val conf = new SparkConf().setAppName("RddPractice").setMaster("local[*]")
// val sc = new SparkContext(conf)
// val rdd = RddPractice

// val lines = sc.textFile("README.md")
// val pLines = sc.textFile("README.md").setName("readme").persist()

// pLines.collect
// pLines.count

// pLines.unpersist(true)
// pLines.collect


// val stopWordCount = sc.accumulator(0: Long)
// val words = sc.textFile("README.md").flatMap(_.split(" "))
// val wordCounts = words.map((_, 1)).reduceByKey(_ + _).filter({w =>
//   val stopWord = Set("a", "an", "for", "in", "on").contains(w._1)
//   if (stopWord) stopWordCount += 1
//   !stopWord
// })

// wordCounts.count
// stopWordCount.value

// val stopWords = sc.broadcast(Set("a", "an", "for", "in", "on"))
// val filtered = words.filter(stopWords.value.contains(_))

// val nums = sc.parallelize(Seq(3, 2, 4, 1, 2, 1), 1)
// nums.glom.mapPartitionsWithIndex((p, e) => e.map((n) => s"""Par$p: ${n.mkString(",")}""")).collect

// val numsPer3 = nums.repartition(3)
// numsPer3.glom.mapPartitionsWithIndex((p, e) => e.map((n) => s"""Par$p: ${n.mkString(",")}""")).collect

// val numsPer2 = numsPer3.coalesce(2)
// numsPer2.glom.mapPartitionsWithIndex((p, e) => e.map((n) => s"""Par$p: ${n.mkString(",")}""")).collect

// val fruitCounts = sc.parallelize(Seq((3, 5), (2, 2), (1, 3), (4, 7)), 3).cache
// val fruitsEn = sc.parallelize(Seq((1, "Apple"), (4, "PineApple"), (3, "Peach"), (2, "Orange")), 3)
// val fruitsJa = sc.parallelize(Seq((2, "オレンジ"), (3, "桃"), (1, "リンゴ"), (4, "パイナップル")), 3)

// val fruitCountsEn = fruitCounts.join(fruitsEn).map({
//   case (id, (count, name)) => (name, count)
// })
// val fruitCountsJa = fruitCounts.join(fruitsJa).map({
//   case (id, (count, name)) => (name, count)
// })


// import org.apache.spark.HashPartitioner

// val fruitCountsP = fruitCounts.
//   partitionBy(new HashPartitioner(sc.defaultParallelism)).cache()

// val fruitCountsEnP = fruitCountsP.join(fruitsEn).map({
//   case (id, (count, name)) => (name, count)
// }).collect
// val fruitCountsJaP = fruitCountsP.join(fruitsJa).map({
//   case (id, (count, name)) => (name, count)
// }).collect

