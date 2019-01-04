package statics

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * //@Author: fansy 
  * //@Time: 2018/12/19 17:11
  * //@Email: fansy1990@foxmail.com
  */
object DescribeStatics {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("Usage: statics.DescribeStatics <input> <hive_table>  <appName>")
      System.exit(-1)
    }
    //
    val (input, table,  appName) = (args(0),args(1),args(2))
    println(new java.util.Date()+": begin spark init...")
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    println(new java.util.Date()+":  spark init done!")

    val data = spark.read.table(input)
    println(new java.util.Date() + ": data.size" + data.count)

    data.describe( data.schema.fieldNames :_*).write.mode(SaveMode.Overwrite).saveAsTable(table)

    spark.stop()
  }
}
