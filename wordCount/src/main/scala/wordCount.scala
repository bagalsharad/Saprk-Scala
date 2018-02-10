import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object wordCount {
  
  def main(args: Array[String]) {
    
    val inputFilePath = "/Users/sharadbagal/BigData/data/"
     val outputFilePath = "/Users/sharadbagal/BigData/data/wc_data"
    
    val conf = new SparkConf().setAppName("word count Program").setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    val words = sc.textFile(inputFilePath +"wordCount.txt",2)
    
    val result = words.flatMap(words => words.split("\\s")).
    map(words => words.replaceAll(".,", "").trim()).
    filter(words => words(0).isLetter).
    filter(words => words == words.toLowerCase).
    map(words => (words,1)).
    reduceByKey((agg, value) => agg + value)
    
    result.collect().foreach(println)
    //result.saveAsTextFile(outputFilePath)
  }
  
}