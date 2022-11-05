package org.fakebob

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object GetTidb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读数据
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://172.31.9.100:4000/fsc_prime")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "prime_offer_info")
      .option("user", "fsc_dataarchive")
      .option("password", "Yumc#1017")
      .load()
    //通过引入隐式转换可以将RDD的操作添加到DataFrame上
    import spark.implicits._

    spark.sql("select * from prime_offer_info limit 10").show()

    spark.close()

  }
}
