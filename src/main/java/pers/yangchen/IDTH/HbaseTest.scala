package pers.yangchen.IDTH

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory


/**
  * Created by yangchen on 16/11/23.
  */
object HbaseTest extends Serializable{
  def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    var table_name = "testlyw"
    var strColFamily1 = "data"
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.zookeeper.quorum", "10.10.240.118")
//    conf.set("hbase.master", "10.10.240.118:60000")
//    conf.addResource("/Users/yangchen/Desktop/bigdata/hbase-site.xml")
val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.master", "localhost:60000")
    conf.addResource("/usr/local/install_package/target_package/hbase-1.0.2/conf/hbase-site.xml")
    conf.set(TableInputFormat.INPUT_TABLE, table_name)

    val tableName = TableName.valueOf(table_name)
//    val hadmin = new HBaseAdmin(conf) 过时的方法
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    if (!admin.isTableAvailable(tableName)) {
      print("表不存在，可以创建表格")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(strColFamily1))
      admin.createTable(tableDesc)
    } else {
      print("表存在，即将进行数据导入")
    }

  }
}
