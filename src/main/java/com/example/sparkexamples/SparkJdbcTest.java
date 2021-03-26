package com.example.sparkexamples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkJdbcTest {
    private static final String DRIVER_CLASS_NAME = "org.h2.Driver";
    private static final String TABLE_NAME = "MYTABLE";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "sa";
    private static final String DB_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";

    public static void main(String[] args) throws SQLException {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));
        // local[*] run on all cpu cores. local[2] means 2 cpu cores.
        SparkSession spark = SparkSession.builder().appName("SparkJdbcTest").master("local[*]")
                .config("spark.driver.bindAddress", "localhost").getOrCreate();
        List<String> data = List.of("""
                "name","age"
                "Ravi",30
                "Sankar",30
                "Challa",2
                """.split("\n"));
        Dataset<String> csvStringSet = spark.createDataset(data, Encoders.STRING());
        Dataset<Row> ds = spark.read().option("header", true).csv(csvStringSet);
        // If name & age is not defined in header or want to replace the incoming header from CSV we can use toDF
        // Dataset<Row> ds = spark.read().csv(csvStringSet).toDF("name", "age");
        ds.printSchema();

        // Create table
        try (Connection con = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
                Statement stm = con.createStatement()) {
            stm.executeUpdate("CREATE TABLE MYTABLE(name varchar, age int)");
        }
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", USERNAME);
        connectionProperties.put("password", PASSWORD);
        ds.write().mode(SaveMode.Overwrite).jdbc("jdbc:h2:mem:testdb", "MYTABLE", connectionProperties);

        Dataset<Row> dbData = spark.read().jdbc(DB_URL,
                "(SELECT * FROM MYTABLE WHERE age=:age) as dataset".replace(":age", "30"),
                connectionProperties);

        dbData.show();
        
        // Creating dataset using java class
        List<Person> data2 = List.of(new Person("Kiran",31), new Person("Kumar",32), new Person("Prabhu",3));
        Dataset<Person> ds2 = spark.createDataset(data2, Encoders.bean(Person.class));

        // Another way of saving & reading the dataset
        ds2.write()
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("driver", DRIVER_CLASS_NAME)
            .option("url", DB_URL)
            .option("dbtable", TABLE_NAME)
            .option("user", USERNAME)
            .option("password", PASSWORD)
            .save();

        Dataset<Row> dbData2 = spark.read()
                    .format("jdbc")
                    .option("driver", DRIVER_CLASS_NAME)
                    .option("url", DB_URL)
                    .option("dbtable", TABLE_NAME)
                    .option("user", USERNAME)
                    .option("password", PASSWORD)
                    .load();
        
        dbData2.show();

    }
    
}

