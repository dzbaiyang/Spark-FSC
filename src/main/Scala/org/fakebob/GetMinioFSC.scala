package org.fakebob

import org.apache.spark
import java.io.InputStream
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import scala.collection.mutable.ListBuffer


object GetMinioFSC {
      // 结果集
      def main(args: Array[String]): Unit = {
            val resList: ListBuffer[(String, String)] = new ListBuffer[Tuple2[String, String]]
            val spark: SparkSession = getSpark()
            //  0 ，隐式转换
            import org.apache.spark.sql.functions._
            import org.apache.spark.sql.types._
            import spark.implicits._

      //  一 ，两表关联
            //  1 ，表计算
            val dfJoin: DataFrame = spark.sql("select * from tests")
            //  2 ，表缓存
            val joinTable: DataFrame = dfJoin.cache()
            //  3 ，表注册 ： joinTable
            joinTable.createOrReplaceTempView("joinTable")
            spark.sql("select * from joinTable").show()
      }

      def getSpark(): SparkSession = {
            //  毫秒
            val timer1: Long = System.currentTimeMillis()
            //  1 ，spark 上下文
            val spark = SparkSession.builder()
              //  为了使用 webUI
              .config("spark.eventLog.enabled", "false")
              //  driver 进程的内存
              .config("spark.driver.memory", "2g")
              //  spark  shuffle numbers
              .config("spark.sql.shuffle.partitions", "100")
              .appName("FakeBobSpark")
              .getOrCreate()
            //  1 ，日志级别
            spark.sparkContext.setLogLevel("WARN")
            //  2 ，读资源文件
            val properties = new Properties()
            val stream: InputStream = GetMinioTest.getClass.getClassLoader.getResourceAsStream("GetMinio.properties")
            properties.load(stream)
            //  3 ，设置数据源 ( s3 )
            val sc: SparkContext = spark.sparkContext
            sc.hadoopConfiguration.set("fs.s3a.access.key", properties.getProperty("fs.s3a.access.key"))
            sc.hadoopConfiguration.set("fs.s3a.secret.key", properties.getProperty("fs.s3a.secret.key"))
            sc.hadoopConfiguration.set("fs.s3a.endpoint", properties.getProperty("fs.s3a.endpoint"))

            //  5 ，注意 ： 读文件，有表头
            val dftest: DataFrame = spark.read.option("header", "true").option("delimiter", ",").csv("s3a://fakebob/test/test.csv")
            val dftests: DataFrame = dftest.toDF("id", "name", "dt")
            dftests.cache().createOrReplaceTempView("tests")
            spark
      }
}