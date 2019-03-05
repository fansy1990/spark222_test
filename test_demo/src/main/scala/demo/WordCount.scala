package demo

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * //@Author: fansy 
  * //@Time: 2018/12/19 11:37
  * //@Email: fansy1990@foxmail.com
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    println("hello world")

    val a = "abc|sdf"
    println(a.split("\\|").size)

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sc.setLogLevel("WARN")
    val data = sc.parallelize(List("abc,a1,9|sdf,a2,0.9|sdf,a3,0.3","sdfds,a3,9.2|sdf,a2,8.2"))
    println(data.map(_.split("\\|").size).reduce((x, y) => x +y))
    println(data.flatMap(_.split("\\|")).count)

    println(data.flatMap(_.split("\\|")).map(_.split(",").apply(0)).distinct().collect().toList)

    println(data.flatMap(_.split("\\|"))
      .map{x => val t = x.split(",");(t(0),t(2).toDouble)}
        .reduceByKey((x,y) => x+y)
      .collect().toList)


    println(data.flatMap(_.split("\\|"))
      .map{x => val t = x.split(",");(t(0),t(2).toDouble)}
      .reduceByKey((x,y) => Math.min(x,y))
      .collect().toList)

    println(data.flatMap(_.split("\\|"))
      .map{x => val t = x.split(",");(t(0),t(2).toDouble)}
      .reduceByKey((x,y) => Math.max(x,y))
      .collect().toList)

    println(data.map{y => val t = y.split("\\|"); t.map(x => x.split(",")).map(x => x(2).toDouble).sum}
      .collect().toList)


    println(data.flatMap(_.split("\\|")).sortBy(x => x.split(",").apply(1)).collect().toList)

    case class MyEvent(product:String, ts:String, amount:Double)
    import sqlContext.implicits._
    val rdd = data.map{x => Row(x.split("\\|"))}
    val schema = new StructType().add(StructField("product_ts_amount",ArrayType(StringType),false))
//    sqlContext.createDataFrame(rdd,sctype)
    val df = sqlContext.createDataFrame(rdd, schema)

    println(df.dtypes)
    df.show(3)

    df.write.mode("Overwrite").saveAsTable("hive_table")

  }
}
