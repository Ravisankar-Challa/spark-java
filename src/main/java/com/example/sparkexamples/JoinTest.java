package com.example.sparkexamples;

import java.util.List;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JoinTest").master("local[*]")
                .config("spark.driver.bindAddress", "localhost").getOrCreate();
        Dataset<String> csvDataString = spark
                .createDataset(List.of("1,Ravi,10",
                        "2,Sankar,20",
                        "3,Kumar,30"), Encoders.STRING());
        Dataset<Row> students = spark.read().csv(csvDataString).toDF("student_id", "s_name", "dep_id");
        students.createOrReplaceTempView("_students");
        spark.table("_students").show();
        
        Dataset<String> depCsvDataString = spark
                .createDataset(List.of("10,ECE,Nellore",
                        "20,EEE,Ongole",
                        "40,CSE,Kadapa"), Encoders.STRING());
        Dataset<Row> departments = spark.read().csv(depCsvDataString).toDF("depart_id", "dep_name", "location");
        departments.show();
        departments.createOrReplaceTempView("_departments");

        // Inner join
        System.out.println("Inner join #################");
        students.join(departments).where("dep_id == depart_id").show();
        students.join(departments).filter("dep_id == depart_id").show();
        students.join(departments, col("dep_id").equalTo(col("depart_id"))).show();
        students.join(departments, students.col("dep_id").equalTo(departments.col("depart_id")), "inner").show();
        spark.sql("select * from _students s inner join _departments d on s.dep_id = d.depart_id").show();
        
        //Left join
        System.out.println("Left join #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "left_outer").show();
        spark.sql("select * from _students s left join _departments d on s.dep_id = d.depart_id").show();
        
        System.out.println("Left join excluding right #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "left_outer").where("depart_id is null").show();
        spark.sql("select * from _students s left join _departments d on s.dep_id = d.depart_id where d.depart_id is null").show();
        
        // Right join
        System.out.println("Right join #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "right_outer").show();
        spark.sql("select * from _students s right join _departments d on s.dep_id = d.depart_id").show();
        
        System.out.println("Right join excluding left #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "right_outer").where("dep_id is null").show();
        spark.sql("select * from _students s right join _departments d on s.dep_id = d.depart_id where s.dep_id is null").show();
        
        // Full outer join
        System.out.println("Full outer join #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "full_outer").show();
        spark.sql("select * from _students s full outer join _departments d on s.dep_id = d.depart_id").show();
        
        System.out.println("Full outer join with out intersection #################");
        students.join(departments, col("dep_id").equalTo(col("depart_id")), "full_outer").where("dep_id is null or depart_id is null").show();
        spark.sql("select * from _students s full outer join _departments d on s.dep_id = d.depart_id where s.dep_id is null or d.depart_id is null").show();
        
        
    }
}
