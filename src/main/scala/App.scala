/* SimpleApp.scala */

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object App {

  case class TipoDeCrime(tipo: String)

  def main(args: Array[String]) {
    val logFile = "test.txt" // Should be some file on your system
    val conf = new SparkConf().setMaster("local[*]").setAppName("MC855 - Spark Jobs")
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile, 2).cache()

    val tiposDeCrime = file.map(t => {
      val parts = t.split(",")
      TipoDeCrime(parts(1))
    })
      .distinct()
      .collect()

    var i = 0
    var tiposDeCrimeMap: Map[String, Int] = Map()
    tiposDeCrime.foreach(t => {
      tiposDeCrimeMap += (t.tipo -> i)
      i += 1
    })

    println(s"tipos de crime: $tiposDeCrimeMap")

    val quantidadeDeCrimes = tiposDeCrimeMap.size

    val parsedData = file.map(s => {
      val parts = s.split(",")
      val tipoDeCrime = parts(1)

      var vetor = Array.fill[Double](quantidadeDeCrimes)(0D)

      val vetorIndex = tiposDeCrimeMap(tipoDeCrime)
      vetor(vetorIndex) = 1

      vetor :+= parts(8).toDouble
      vetor :+= parts(7).toDouble

      Vectors.dense(vetor)
    }).cache()

    val numClusters = quantidadeDeCrimes
    val numInterations = 100
    val clusters = KMeans.train(parsedData, numClusters, numInterations)

    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within set sum of squared errors = $WSSSE")
    clusters.save(sc, "output")
    val sameModel = KMeansModel.load(sc, "output")
    println("finish")
  }
}
