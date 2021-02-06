package com.example.sparkexamples;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveTest {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example").master("local[*]")
                .config("spark.driver.bindAddress", "localhost")
                .config("hive.exec.scratchdir", "C:\\ProgramData\\tmp\\hive\\")
                // .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/hive/warehouse")
                .enableHiveSupport().getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("fs.defaultFS",
                "file:///C:/ProgramData/tmp/mylocalhdfs_spark");

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");
        spark.sql("SELECT * FROM src").show();
        spark.sql("SELECT * FROM src").map((MapFunction<Row, String>) row -> "Key: "
                + row.<Integer>getAs("key") + " Value: " + row.getString(1), Encoders.STRING()).show();
        spark.sql("CREATE TABLE IF NOT EXISTS csv_test (name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' STORED AS TEXTFILE");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/hive_data_test.csv' OVERWRITE INTO TABLE csv_test");
        spark.sql("SELECT * FROM csv_test").show();
    }

}
