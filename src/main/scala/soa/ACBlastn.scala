package soa

import io.Automaton
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ArrayBuffer

object ACBlastn {

  def deleteCR(block : Iterator[String], SIZE_PART_DB: LongAccumulator) : Iterator[String] = {
    val str : StringBuffer = new StringBuffer("")
    while (block.hasNext) {
      val read = block.next
      str.append(read.substring(0, read.length -1))
      SIZE_PART_DB.add(read.length.toLong)
    }
    val array : Array[String] = Array[String] (str.toString)
    array.toIterator
  }

  def searchPartitions (index : Int, block : Iterator[String], queryPart : Array[String]) : Iterator[(String, Int)] = {
    val array : ArrayBuffer[(String, String)] = ArrayBuffer()
    // create AC automaton
    val ac = new Automaton
    ac.setFailTransitions()
    //creazione struttura
    queryPart.foreach(ac.addWord("query", _))
    //ricerca
    while (block.hasNext)
      array ++= ac.search(block.next)
    array.map(x => (x._1, 1)).toIterator
  }

  def main(args : Array[String]) : Unit = {

    val spark = SparkSession.builder.
      appName("ACBlastn").
      master(if (args(0) == "local") "local[*]" else "yarn").
      getOrCreate()

    val sc = spark.sparkContext

    // load query file
    val query = sc.textFile(args(2)).collect
    // load db file
    val db = sc.textFile(args(1), sc.defaultParallelism)

    val SIZE_PART_DB = sc.longAccumulator("SIZE_PART_DB")

    val dbFile = db.mapPartitions(deleteCR(_, SIZE_PART_DB))

    val dbCache = dbFile.cache()

    println("DB SIZE PART "+SIZE_PART_DB)
    var count = 0
    var tmpSize = 0
    val blockSize = 1048576 * 10
    while (count < query.size && tmpSize <= blockSize) {
      tmpSize += (query(count).getBytes.length + 1)
      count = count + 1
    }

    var i = 0
    val parts = query.grouped(count) // partizioni da circa 16mb
    while (parts.hasNext) {
      val part = parts.next
      val tmp = dbCache.mapPartitionsWithIndex(searchPartitions(_, _, part))
      tmp.reduceByKey(_+_).
        saveAsTextFile(s"resultQueryPart-${i}")
      i = i + 1
    }

    spark.stop
  }
}