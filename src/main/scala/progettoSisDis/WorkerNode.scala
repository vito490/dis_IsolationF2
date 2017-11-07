package progettoSisDis

import org.apache.spark.sql._
import org.apache.spark._

object WorkerNode {

  def cutting(data: DataFrame, maxprojections: Int, soglia: Int) {
    var j = 10
    var mp = maxprojections
    var i = 0
    var previousI = i
    var filtered = data
    filtered.createOrReplaceTempView("dataset")
    val cols = data.columns
    cols.foreach(println)
    while (mp > 0) {
      println(
        "***********************************************************sono vivo**********************************")
      filtered = filtered.sqlContext.sql(
        "SELECT * FROM dataset WHERE " + cols(i) + " > " + soglia)
      i = (i + 1) % cols.length
      mp = mp - 1

    }

    println(
      "*********valore di maxproj = " + mp + "******************************")

  }

}
