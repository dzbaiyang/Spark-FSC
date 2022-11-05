package GetMinIO
import java.io.InputStream
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import scala.collection.mutable.ListBuffer


object GetMinioAndCalcToMinio {
  def main(args: Array[String]): Unit = {

    //  goods("typeColorId","typeId","bigLei","midLei","smallLei","gendar","yearSeason","goodsYear","goodsSeason","color","sellMonth","sellTpt","dingScore","yunYingScore","boDuan","sellPrice","serialId","banType")
    //  stock("area","stockId","sellMonth","sellDate","kuanId","color","reasonBian","numBian","owner","stockType")
    //  结果集
    val resList: ListBuffer[(String, String)] = new ListBuffer[Tuple2[String, String]]
    val spark: SparkSession = getSpark()
    //  0 ，隐式转换
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._

    //  一 ，两表关联 ( goods,stock )
    //  1 ，表关联 ： ( 区域，小类，款色号，日期-此时已经转变为第几周，库存变动量 ),把日期转换为第几周
    val dfJoin: DataFrame = spark.sql("select stock.area,goods.smallLei,goods.typeColorId,weekofyear(to_date(stock.sellDate,'yyyyMMdd')) weekofyear,stock.numBian " +
      "from lifeCycleGoods goods,lifeCycleStock stock where goods.typeId=stock.kuanId and goods.color=stock.color")
    //  2 ，表缓存
    val joinTable: DataFrame = dfJoin.cache()
    //  3 ，表注册 ： joinTable ( stock.area,smallLei,typeColorId,weekofyear,numBian )
    joinTable.createOrReplaceTempView("joinTable")

    //  二 ，周聚合 ： dfZhou ( area,smallLei,typeColorId,weekofyear,sumstock )
    //  1 ，周 sql
    val dfZhou: DataFrame = spark.sql("select area,smallLei,typeColorId,weekofyear,sum(numBian) sumstock from joinTable group by area,smallLei,typeColorId,weekofyear order by area,smallLei,typeColorId,weekofyear")
    //  2 ，周缓存 ：
    val zhouTable: DataFrame = dfZhou.cache()
    //  3 ，周注册 ： ( area,smallLei,typeColorId,weekofyear,sumstock )
    zhouTable.createOrReplaceTempView("zhouTable")
    //  三 ，周累加，结果排序 ( over 里面 order by 到了谁，就按照谁来累加 )
    //  1 ，累加 sql
    var sqlLeiJia = "select " +
      "area,smallLei,typeColorId,weekofyear,sumstock," +
      "sum(sumstock) over(partition by area,smallLei,typeColorId order by area,smallLei,typeColorId,weekofyear) sumoverstock " +
      "from zhouTable " +
      "order by area,smallLei,typeColorId,weekofyear"
    //  2 ，执行，并且，将结果缓存 ( area,smallLei,typeColorId,weekofyear,sumstock,sumoverstock )
    val dfRes: DataFrame = spark.sql(sqlLeiJia).cache()

    //  四 ，将结果重新分区 ( 目的 ： 分文件输出 ) 思路 ： df->rdd->重新分区->rdd->df ( 用 area 分区 )
    //  1 ，所有的 area ，得到一个数组
    val dfAreaRes: DataFrame = spark.sql("select distinct(area) area from joinTable")
    val arr: Array[String] = dfAreaRes.rdd.map(row => {
      row.get(0).toString
    }).collect()
    //  2 ，自定义分区器 ： 按照 area 分区
    val areaPartitioner: StockPartitioner = new StockPartitioner(arr)
    //  3 ，转换 df -> rdd ( area,smallLei,typeColorId,weekofyear,sumstock,sumoverstock )
    val rddRow: RDD[Row] = dfRes.rdd
    //      转换 ： area,smallLei,typeColorId,weekofyear,sumstock
    val rddColumn: RDD[(String, (String, String, String, String, String, String))] = rddRow.map(row => {
      val area: String = row.get(0).toString
      val smallLei: String = row.get(1).toString
      val typeColorId: String = row.get(2).toString
      val weekofyear: String = row.get(3).toString
      val sumstock: String = row.get(4).toString
      val sumoverstock: String = row.get(5).toString
      (area, (area, smallLei, typeColorId, weekofyear, sumstock, sumoverstock))
    })
    //  4 ，重新分区
    val rddPar: RDD[(String, (String, String, String, String, String, String))] = rddColumn.partitionBy(areaPartitioner)
    //  5 ，转换 rdd -> df
    val dfResRes: DataFrame = rddPar.map(e => e._2).toDF("area", "smallLei", "typeColorId", "weekofyear", "sumstock", "sumsumstock")

    //  五 ，输出
    dfResRes.write.option("header", "true").option("delimiter", ",").csv("s3a://lifecyclebigdata/test/data/lifecycle/res02")
    spark.stop()
  }

  //  建 spark 对象
  //  建表 goods ： lifeCycleGoods ( 18 列 )
  //  建表 stock ： lifeCycleStock ( 10 列 )
  def getSpark(): SparkSession = {
    //  毫秒
    val timer1: Long = System.currentTimeMillis()
    //  1 ，spark 上下文
    val spark = SparkSession.builder()
      //  为了使用 webUI
      .config("spark.eventLog.enabled", "false")
      //  driver 进程的内存
      .config("spark.driver.memory", "2g")
      //  spark 的 shuffle 数量
      .config("spark.sql.shuffle.partitions", "100")
      .appName("SparkDemoFromS3")
      .getOrCreate()
    //  1 ，日志级别
    spark.sparkContext.setLogLevel("WARN")
    //  2 ，读资源文件
    val properties = new Properties()
    val stream: InputStream = GetMinioAndCalcToMinio.getClass.getClassLoader.getResourceAsStream("s3.properties")
    properties.load(stream)
    //  3 ，设置数据源 ( s3 )
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.access.key", properties.getProperty("fs.s3a.access.key"))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", properties.getProperty("fs.s3a.secret.key"))
    sc.hadoopConfiguration.set("fs.s3a.endpoint", properties.getProperty("fs.s3a.endpoint"))
    //  4 ，隐式转换
    //  5 ，商品表，18 列 ( 款色号,款号,大类,中类,小类,性别,年季,商品年份,商品季节,颜色,上市月份,上市温度,订货评分,运营评分,波段,售价,系列,版型 )
    //  5 ，注意 ： 读文件，有表头
    val dfSourceGoods: DataFrame = spark.read.option("header", "true").option("delimiter", ",").csv("s3a://lifecyclebigdata/test/data/lifecycle/goods.csv")
    val dfGoods: DataFrame = dfSourceGoods.toDF("typeColorId", "typeId", "bigLei", "midLei", "smallLei", "gendar", "yearSeason", "goodsYear", "goodsSeason", "color", "sellMonth", "sellTpt", "dingScore", "yunYingScore", "boDuan", "sellPrice", "serialId", "banType")
    dfGoods.cache().createOrReplaceTempView("lifeCycleGoods")
    //  6 ，业绩表，10 列 ( 区域,门店代码,销售月份,销售日期,款号,颜色,变动原因,库存变动量,店主,店铺类型 )
    //  6 ，注意 ： 读文件，有表头
    val dfSourceStock: DataFrame = spark.read.option("header", "true").option("delimiter", ",").csv("s3a://lifecyclebigdata/test/data/lifecycle/stock.csv")
    val dfStock: DataFrame = dfSourceStock.toDF("area", "stockId", "sellMonth", "sellDate", "kuanId", "color", "reasonBian", "numBian", "owner", "stockType").withColumn("numBian", col("numBian").cast(IntegerType))
    dfStock.cache().createOrReplaceTempView("lifeCycleStock")
    spark
  }
}
