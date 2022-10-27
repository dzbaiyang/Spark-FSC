package GetMinIO

import org.apache.spark
import org.apache.spark.sql.SparkSession

object GetMinio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .appName("SparkDemoFromS3")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "")
    val rdd = spark.sparkContext.textFile("s3a://xxx/part-00000")

    println(rdd.count())

  }

}
