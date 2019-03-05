package statics

import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * //@Author: fansy 
  * //@Time: 2018/12/19 17:11
  * //@Email: fansy1990@foxmail.com
  */
object DescribeStaticsMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //
    val (input, table,  appName) = ("default.demo_600w",
      "default.demo_600w_statics"," describe "+new Date())
    println(new java.util.Date()+": begin spark init...")
    val spark = SparkSession.builder().master("spark://node200:7077")
      .config("hive.metastore.uris", "thrift://node200:9083")
      .config("spark.executor.memory","768M")
      .appName(appName).enableHiveSupport().getOrCreate()
    println(new java.util.Date()+":  spark init done!")

    val data = spark.read.table(input)
    println(new java.util.Date() + ": data.size" + data.count)

    data.describe( data.schema.fieldNames :_*).write.mode(SaveMode.Overwrite).saveAsTable(table)

    spark.stop()
  }
}
