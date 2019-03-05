package justspark

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * author : fanzhe
  * email : fansy1990@foxmail.com
  * date : 2019/1/4 PM9:39.
  */
object SparkNothingMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //
    val (use_hive, table,  appName) = (true,"a","just for test")
    println(new java.util.Date()+": begin spark init...")
    val spark = SparkSession.builder().master("spark://node200:7077").appName(appName)
        .config("hive.metastore.uris", "thrift://node200:9083")
        .config("spark.executor.memory","768M")
      .enableHiveSupport().getOrCreate()
    println(new java.util.Date()+":  spark init done!")

    if(use_hive) {
      spark.createDataFrame(List((1, "0"))).write.mode(SaveMode.Overwrite).saveAsTable(table)
    }
    spark.stop()
  }
}
