package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by 史红光 on 2019/9/21.
  */
object CreateDFFromJsonFile {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .master("local")
                .appName("jsonRDD")
                .getOrCreate()
        val jsonDS: DataFrame = spark.read.json("./data/json")
	//史红光
        jsonDS.show()

        jsonDS.createOrReplaceTempView("jsonTable")

        val result: DataFrame = spark.sql("select * from jsonTable where age is not NULL")

        result.show()

        spark.stop()

    }

}
