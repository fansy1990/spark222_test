package prepare

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 导入数据到Hive
  * //@Author: fansy 
  * //@Time: 2018/12/19 13:59
  * //@Email: fansy1990@foxmail.com
  */
object Load2Hive {
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      println("Usage: prepare.Load2Hive <input> <hive_table>")
      System.exit(-1)
    }
    //
    val (input, table) = (args(0),args(1))

    val spark = SparkSession.builder().appName("Load demo data to hive").enableHiveSupport().getOrCreate()

    val df =spark.read.format("csv").option("header", "true").option("inferSchema","true").load(input)

    df.write.mode(SaveMode.Overwrite).saveAsTable(table)

    spark.stop()
  }
}
