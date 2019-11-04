package com.bde

import java.net.InetAddress

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import py4j.GatewayServer
import org.apache.spark.storage.StorageLevel
import java.util.UUID.randomUUID

import org.apache.spark

object CustomSparkConf {

  val localSparkConf = new SparkConf().setMaster("local").
    set("spark.sql.execution.arrow.enabled", "true").
    set("spark.sql.execution.arrow.pyspark.enabled", "true").
    setAppName("alexApp1")
  val localSparkConf2 = new SparkConf().setMaster("local").setAppName("alexApp1")
  val sparkSession: SparkSession = SparkSession.builder.config(localSparkConf).getOrCreate()


  val sparkContext = sparkSession.sparkContext

  val sqlContext = sparkSession.sqlContext


  val sparkConf = sparkSession.sparkContext.getConf

  val javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext)

}

object PythonEntryPoint {

  def getJavaSparkConf(): SparkConf = CustomSparkConf.sparkConf

  def getSparkSession(): SparkSession = CustomSparkConf.sparkSession

  def getJavaSparkContext: JavaSparkContext = CustomSparkConf.javaSparkContext

  def getJavaSQLContext: SQLContext = CustomSparkConf.sqlContext

}

object MyApp extends App {
  println(args)
  var exportParquet = false
  var parquetFolder = "./out"
  var nrOfEntries = 100000
  var givenHost = "127.0.0.1"
  if (args.length > 0) {
    if (args(0) == "export" && args(1).length > 0) {
      exportParquet = true
      parquetFolder = args(1)
      nrOfEntries = args(2).toInt
    }
    else {
      givenHost = args(0)
    }
  }
  val sparkSession: SparkSession = CustomSparkConf.sparkSession //SparkSession.builder.appName("alexApp1").config("spark.master", "local").getOrCreate()


  val sql = sparkSession.sqlContext

  val bigList = Range(0, nrOfEntries).map(x => List(randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString,
    randomUUID().toString)).toList
  val values = bigList.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19)))

  import sql.implicits._

  val df: DataFrame = values.toDF(Range(0, 20).map(x => "col" + x.toString): _*)


  if (exportParquet) {
    "none,uncompressed,snappy,gzip,lzo".split(",").toList.map { codec =>
      df.repartition(10).write.mode(SaveMode.Overwrite).option("spark.sql.parquet.compression.codec", codec).parquet(parquetFolder + "/demo_" + codec + ".parquet")
    }
    System.exit(0)
  }
  else {
    val gatewayServer: GatewayServer = {
      val inetAddress = InetAddress.getByName(givenHost)
      println(s"Start Gateway server with host: ${inetAddress} and port 25333")
      val gateway_server = new GatewayServer(PythonEntryPoint, 25333, 0, inetAddress, null, 0, 0, null); // scalastyle:ignore
      Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
        override def run(): Unit = {
          print("Application is being shutted down")
          try {
            print("Shutting down gatewayServer")
            gateway_server.shutdown()
          } catch {
            case t: Throwable =>
              print("Failed to stop gatewayServer", t)
          }
        }
      }))
      gateway_server
    }


    df.createOrReplaceTempView("myTempTable")

    val allTables = sparkSession.catalog.listTables()
    println(sparkSession.catalog.tableExists("myTempTable"))

    sparkSession.sharedState.cacheManager.cacheQuery(sparkSession.table("myTempTable"), Some("myTempTable"), StorageLevel.MEMORY_AND_DISK)
    print(sparkSession.table("myTempTable").count())
    gatewayServer.start()
    println("---------------------------------------------")
    println(sql.sql("show tables").collect().map(row => row.get(0).toString).mkString(","))
    println("---------------------------------------------")
    //println(sql.sql("select * from myTempTable").collect().map(row=>row.toString).mkString(","))
    println("---------------------------------------------")
    println(gatewayServer)

    System.in.read()
  }


}