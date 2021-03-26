package com.example.sparkexamples;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RddDataSetWithFiltersExample {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));
        SparkSession spark = SparkSession.builder().appName("RddDataSetWithFiltersExample").master("local[*]")
                .config("spark.driver.bindAddress", "localhost").getOrCreate();
        JavaRDD<String> lines = spark.read().textFile("src/main/resources/test.csv").javaRDD();
        var parts = lines.map(line -> line.split(","));
        JavaRDD<Object> peopleRDD = parts.map(part -> new Person(part[0], Integer.parseInt(part[1].trim())));
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.show();

        peopleDF.withColumn("new_column", 
                when(col("age").notEqual(30), date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                .otherwise(null)).show(false);
        peopleDF.withColumn("new_column", 
                when(col("age").notEqual(30), to_date(lit("2021-02-06"), "yyyy-MM-dd"))
                .otherwise(null)).show(false);

        peopleDF.filter("age != 30").show();
        peopleDF.filter("age >= 0  and age <= 9").show();
        peopleDF.filter(col("age").geq(0).and(col("age").leq(9))).show();
        peopleDF.filter(peopleDF.col("age").geq(0).and(peopleDF.col("age").leq(9))).show();
        peopleDF.drop("name").show();
        peopleDF.drop(col("name")).show();
        peopleDF.drop(peopleDF.col("name")).show();
        peopleDF.select("name").show();
        peopleDF.where(col("age").between("0", "9")).show();
        
        peopleDF.createOrReplaceTempView("people");
        var teenagers = spark.sql("SELECT name FROM people WHERE age >= 0 AND age <= 9");
        teenagers.show();
        
        teenagers.map((MapFunction<Row, String>) row -> "Name: " + row.getAs("name"), Encoders.STRING()).show();
    }
}
