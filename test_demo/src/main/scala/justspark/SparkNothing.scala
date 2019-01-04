package justspark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * author : fanzhe
  * email : fansy1990@foxmail.com
  * date : 2019/1/4 PM9:39.
  */
object SparkNothing {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("Usage: justspark.SparkNothing <use_hive_or_not> <hive_table>  <appName>")
      System.exit(-1)
    }
    //
    val (use_hive, table,  appName) = (args(0).toBoolean,args(1),args(2))
    println(new java.util.Date()+": begin spark init...")
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    println(new java.util.Date()+":  spark init done!")

    if(use_hive) {
      spark.createDataFrame(List((1, "0"))).write.mode(SaveMode.Overwrite).saveAsTable(table)
    }
    spark.stop()
  }
}
