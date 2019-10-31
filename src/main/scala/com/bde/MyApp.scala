package com.bde

import java.net.InetAddress


import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import py4j.GatewayServer


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

  val gatewayServer: GatewayServer = {
    val inetAddress = InetAddress.getByName("10.1.100.173")
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

  val sparkSession: SparkSession = CustomSparkConf.sparkSession //SparkSession.builder.appName("alexApp1").config("spark.master", "local").getOrCreate()


  val sql = sparkSession.sqlContext

  val bigList = Range(0, 50000).map(x => List(x.toString, "s" + x.toString)).toList
  val values = bigList.map(x => (x(0), x(1)))

  import sql.implicits._

  val df: DataFrame = values.toDF("col1", "col2")

  import org.apache.spark.storage.StorageLevel

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