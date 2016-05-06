/* SimpleApp.scala */

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import purecsv.unsafe._

object App {

  case class CsvLine(c1: String, tipo: String, c3: String, c4: String, c5: String, c6: String, c7: String, lon: Double, lat: Double)

  def main(args: Array[String]) {
    val fileCsv = "crimes.csv" // Should be some file on your system
    val conf = new SparkConf().setMaster("local[*]").setAppName("MC855 - Spark Jobs")
    val sc = new SparkContext(conf)
    val file = sc.textFile(fileCsv, 2).cache()
    val header = file.first() // extract header

    val lines = file.filter(x => x != header).map(t => CSVReader[CsvLine].readCSVFromString(t).head).cache()
    val tiposDeCrime = lines.map(t => t.tipo)
      .distinct()
      .collect()

    var i = 0
    var tiposDeCrimeMap: Map[String, Int] = Map()
    tiposDeCrime.foreach(t => {
      tiposDeCrimeMap += (t -> i)
      i += 1
    })

    println(s"tipos de crime: $tiposDeCrimeMap")

    val quantidadeDeCrimes = tiposDeCrimeMap.size

    val parsedData = lines.map(s => {
      val tipoDeCrime = s.tipo

      var vetor = Array.fill[Double](quantidadeDeCrimes)(0D)

      val vetorIndex = tiposDeCrimeMap(tipoDeCrime)
      vetor(vetorIndex) = 1

      vetor :+= s.lat
      vetor :+= s.lon

      Vectors.dense(vetor)
    }).cache()

    val numClusters = quantidadeDeCrimes
    val numInterations = 100
    val clusters = KMeans.train(parsedData, numClusters, numInterations)

//    val WSSSE = clusters.computeCost(parsedData)
//    println(s"Within set sum of squared errors = $WSSSE")
//    clusters.save(sc, "output")
//    val sameModel = KMeansModel.load(sc, "output")

    println(s"Result = ${clusters.clusterCenters}")
  }
}
