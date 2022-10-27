package GetMinIO

import org.apache.spark.Partitioner

class StockPartitioner(val arr: Array[String]) extends Partitioner  {
  //  理论 ：
  //      1 ，分区从 0 开始
  //      2 ，一共 num 个分区的话，分区号从 0 到 num-1
  //  1 ，分区数 ( 有几个区域，就有几个分区 )
  override def numPartitions:Int = arr.length
  //  2 ，根据 key ，指定分区
  override def getPartition(key: Any):Int = {
    return arr.indexOf(key.toString)
  }
}