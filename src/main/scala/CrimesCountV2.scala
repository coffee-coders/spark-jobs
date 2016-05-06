/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}
import purecsv.unsafe._

object CrimesCountV2 {

  case class CsvLine(c1: String, tipo: String, c3: String, c4: String, c5: String, c6: String, c7: String, lon: Double, lat: Double)

  case class Point(x: Double, y: Double)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MC855 - Spark Jobs")
    val sc = new SparkContext(conf)
    val file = sc.textFile("crimes_2.csv", 2).cache()
    val header = file.first() // extract header

    val lines = file.filter(x => x != header).map(t => CSVReader[CsvLine].readCSVFromString(t).head).cache()

    val allLines = sc.broadcast(lines.collect())

    val result = lines.flatMap(leftT => allLines.value.filter(rightT => rightT.tipo == leftT.tipo)
      .map(rightT => (leftT, rightT)))
      .map { case (x, y) => (x, distance(Point(x.lat, x.lon), Point(y.lat, y.lon))) }
      .filter(t => t._2 < 4000D)
      .map(t => (t._1, 1))
      .reduceByKey(_ + _)
      .map(t => (t._1.tipo, t._2))
      .reduceByKey((a, b) => if (a > b) a else b)
      .collect()

    result.foreach(t => {
      println(s"Result ${t._1}: ${t._2}")
    })
  }

  def distance(a: Point, b: Point): Double = {
    // http://andrew.hedges.name/experiments/haversine/

    val earthRadius: Double = 6371000 // meters
    val dLat: Double = Math.toRadians(a.x - b.x)
    val dLng: Double = Math.toRadians(a.y - b.y)
    val result: Double = Math.pow(Math.sin(dLat / 2), 2) +
      Math.cos(Math.toRadians(a.x)) * Math.cos(Math.toRadians(b.x)) * Math.pow(Math.sin(dLng / 2), 2)
    val c: Double = 2 * Math.atan2(Math.sqrt(result), Math.sqrt(1 - result))
    earthRadius * c
  }
}
