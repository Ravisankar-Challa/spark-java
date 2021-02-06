package com.example.sparkexamples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlDirectTestOnFile {
    public static void main(String[] args) {
        // Read a json file with out using spark.read()
        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlDirectTestOnFile")
                .master("local[*]")
                .config("spark.driver.bindAddress", "localhost")
                .getOrCreate();
        
        Dataset<Row> ds = spark.sql("SELECT * FROM json.`src/main/resources/test.json`");
        ds.show();
    }
}
