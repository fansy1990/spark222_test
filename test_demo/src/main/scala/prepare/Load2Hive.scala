package prepare

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 导入数据到Hive
  * //@Author: fansy 
  * //@Time: 2018/12/19 13:59
  * //@Email: fansy1990@foxmail.com
  */
object Load2Hive {
  def main(args: Array[String]): Unit = {
    if(args.length !=5){
      println("Usage: prepare.Load2Hive <input> <hive_table> <times> <appName> <partitionColumn>")
      System.exit(-1)
    }
    //
    val (input, table, num , appName, partitionColumn) = (args(0),args(1), args(2).toInt,args(3),args(4))

    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()

    val df =spark.read.format("csv").option("header", "true").option("inferSchema","true").load(input)

    Array.fill[DataFrame](num)(df).reduce(_ union _).write.partitionBy(partitionColumn).mode(SaveMode.Overwrite).saveAsTable(table)

    spark.stop()
  }
}
