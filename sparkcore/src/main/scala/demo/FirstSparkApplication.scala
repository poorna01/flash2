package demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FirstSparkApplication {
  def main(args:Array[String]){
    val conf=new SparkConf().setMaster("local").setAppName("FirstSparkApplication")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val r1=sc.textFile("F:\\data\\emp.csv")
    val r2=r1.map(x=>x.split(",")).map(x=>(x(7),x(5).toInt)).reduceByKey(_+_)
    r2.foreach(println)
    
  }
}