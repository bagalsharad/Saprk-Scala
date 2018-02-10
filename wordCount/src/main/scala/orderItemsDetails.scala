
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem,Path}


case class Orders(order_id: Int,
    order_date: String,
    order_customer_id: Int,
    order_status: String)
    
case class OrderItems(order_item_id: Int,
    order_item_order_id: Int,
    order_item_product_id: Int,
    order_item_product_qty: Int,
    order_item_subtotal: Double,
    order_item_product_price: Double)
    
    
object orderItemsDetails {
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("order items").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    
    
    import sqlContext.implicits._
    
    val inputFilePath ="/Users/sharadbagal/BigData/data/retail_db/"
    val outputFilePath = "/Users/sharadbagal/BigData/data/scv"
    
    val ip= new Path(inputFilePath)
    val op  = new Path(outputFilePath)
    
    if (!fs.exists(ip)){
      println("Base Directory does not exists")
      return
    }
    
    if(fs.exists(op)){
      fs.delete(op,true)
    }
        
        
    val ordersDF = sc.textFile(inputFilePath +"orders").
    map(rec => {
      val r = rec.split(",")
      Orders(r(0).toInt,r(1),r(2).toInt,r(3))
     }).toDF()
     
     val orderItemsDF = sc.textFile(inputFilePath +"order_items").
     map(rec => {
       val r = rec.split(",")
       OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toDouble, r(5).toDouble)
     }).toDF()
     
     val orderDeails = ordersDF.join(orderItemsDF,ordersDF("order_id") === orderItemsDF("order_item_order_id"))
     
     orderDeails.show()
  }
}