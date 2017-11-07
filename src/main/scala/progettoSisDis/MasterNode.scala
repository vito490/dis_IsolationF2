package progettoSisDis
import scala.math
import org.apache.spark.sql.functions.{min, max}

import org.apache.spark.sql._
import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import scala.io._
import progettoSisDis.ReturnType
import org.apache.log4j.lf5.LogLevel
import org.slf4j.Logger
import scala.concurrent.Await

object MasterNode {
  var set: ArrayBuffer[String] = new ArrayBuffer[String]()
  val r1 = new scala.util.Random
  val conf = new SparkConf()
    .setAppName("Isolation Forest Distribuito")
    .setMaster("local[*]")
    .set("spark.executors.core.instances", "4")
    .set("spark.dynamicAllocation.enabled", "true")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)

  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.eventLog.dir", "/tmp/spark-events")

  var features: ArrayBuffer[String] = new ArrayBuffer[String]
  def SetBuilding(features: ArrayBuffer[String],
                  soglia: Int): ArrayBuffer[String] = {
    if (soglia > features.length)
      return null

    this.features = features
    recursiveBuilding(0, soglia, "")
    return set
  }

  private def recursiveBuilding(currIndex: Int, soglia: Int, currAttr: String) {
    if (soglia < 0)
      return
    var i = currIndex
    if (soglia == 1) {
      while (i < features.length) {
        var s = currAttr + "," + features(i)
        set.+=(s)
        i = i + 1
      }
    }
    while (i < features.length) {
      var s = ""
      if (currAttr.equals("")) {
        s = features(i)
      } else {
        s = currAttr + "," + features(i)
      }

      set.+=(s)
      recursiveBuilding(i + 1, soglia - 1, s)

      i = i + 1

    }

  }

  def preprocessing() {
    println(
      "***************************CARICAMENTO DATASET****************************************")

    val path = "/home/vito/vinorosso2.csv"
    var df = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load(path)

    df.registerTempTable("table")
    println(
      "*************************INFERENZA SCHEMA********************************************")

    val schema = df.schema
    val features = new ArrayBuffer[String]()
    for (i <- schema)
      if (i.dataType.equals(sql.types.IntegerType) || i.dataType.equals(
            sql.types.DoubleType)
          || i.dataType.equals(sql.types.FloatType) || i.dataType.equals(
            sql.types.ShortType))
        features.+=(i.name)

    println(
      "*****************PROIEZIONE SU ATTRIBUTI NUMERICI E FILTREAGGIO***********************")
    var index: Int = 0
    var set = SetBuilding(features, 3)
    val soglia: Int = 30
    var cuts: Array[Double] = new Array(set.length)
    var maxprojections = 5
    var exit = false
    while (index < 2) {

      println(
        "**************VALUTAZIONE SOTTOINSIEME: " + set(index) + "***********************")

      var data = sqlContext.sql("SELECT " + set(1) + " FROM table")
      cuts(index) = {

        var mp = 0
        var i = 0
        var fakeval = r1.nextInt(40)
        val cols = data.columns
        cols.foreach(println)

        val r = new scala.util.Random
        var datax = null
        while (data.count() != 0 && mp <= maxprojections) {
          println(
            "********************************" + cols(i) + "************************")
          //if (dim == 1)
          // mp
          var Row(minValue: Any, maxValue: Any) =
            data.agg(min(cols(i)), max(cols(i))).head()
          println(minValue + " " + maxValue)
          if (minValue == maxValue)
            2 * (scala.math.log(data.count() + 0.5772)) - 2
          var maxs = ReturnType(maxValue)
          var mins = ReturnType(minValue)
          var threshh = mins.get() + r.nextInt(
            maxs.get().toInt - mins.get().toInt + 1)
          data.foreach(println(_))

          if (threshh >= fakeval)
            data = data.filter(((elem: Row) =>
              ReturnType(elem(i)) < threshh.toDouble))
          else
            data =
              data.filter((elem: Row) => ReturnType(elem(i)) > threshh.toDouble)

          i = (i + 1) % cols.length
          mp = mp + 1
          println("*************************** PROJECTIONS: " + mp)
          data.foreach(println(_))

        }
        mp
      }
      index += 1
    }
    cuts.foreach(println)
    exit = true

  }
  def main(args: Array[String]) {
    preprocessing()
    var r = ""
    readLine()
  }

}
