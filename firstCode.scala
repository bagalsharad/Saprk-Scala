
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object firstCode {
  def main(args: Array[String]) {
    val logFile = "/Users/sharadbagal/spark/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("firstCode").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    
    val wordCount = logData.flatMap(words => words.split(" ")).
                            //filter(words => words(0).isLetter).
                            filter(words => words == words.toUpperCase()).
                            map(words => (words, 1)).
                            reduceByKey((agg, value)=> agg + value)
        
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("count of words" + wordCount.count())
    wordCount.collect().foreach(println)
  }
}