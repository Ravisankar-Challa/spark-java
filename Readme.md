H2 has been used as in memory database for tetsing SparkJdbc

How to run spark in distributed mode

```
To start spark master
spark-class org.apache.spark.deploy.master.Master

To start spark worker
spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077

To start spark shell
spark-shell --master spark://localhost:7077
```

How to change the driver bind address

```
var spark = SparkSession.builder.config("spark.driver.bindAddress", "localhost").getOrCReate();
```

If you are running spark in standalone mode and want to test hive capabilities using enableHiveSupport()

```
var spark = SparkSession.builder.config("hive.exec.scratchdir", "C:\\tmp\\hive\\").enableHiveSupport().getOrCReate();
spark.sparkContext().hadoopConfiguration().set("fs.defaultFS", "file:///C:/tmp/mylocalhdfs_spark");
```

Creating dataset using java class

```
List<Person> data2 = List.of(new Person("Kiran",31), new Person("Kumar",32), new Person("Prabhu",3));
Dataset<Person> ds2 = spark.createDataset(data2, Encoders.bean(Person.class));
```

Different types of joins and their values

```     
"inner",
"outer", "full", "fullouter", "full_outer",
"leftouter", "left", "left_outer",
"rightouter", "right", "right_outer",
"leftsemi", "left_semi", "semi",
"leftanti", "left_anti", "anti",
"cross"
```

pyspark  example

```
set JAVA_HOME=C:\ProgramData\Softwares\jdk1.8.0_262
pyspark --conf spark.driver.host=localhost --files file:///C:/apache-hadoop/tools/hadoop-3.1.1/etc/hadoop/core-site.xml,file:///C:/apache-hive/tools/apache-hive-3.1.2-bin/conf/hive-site.xml --jars file:///C:/spark_libs/*,file:///C:/apache-phoenix-5.0.0-HBase-2.0-bin/*,file:///C:/apache-hive-3.1.2-bin/lib/*

from pyspark.sql.functions import *
from pyspark.sql import *
spark = SparkSession.builder.config('hive.exec.scratchdir', 'C:/tmp/hive').config('spark.driver.bindAddress', 'localhost').enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("INFO")
spark.read.orc("/tmp/data/year=2020/month=08/day=01/hour=00/")
spark.createDataFrame([Row(id= 1, name= 'Ravi'), Row(id= 2, name= 'Sankar')])
spark.sql("CREATE TABLE IF NOT EXISTS csv_test (name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' STORED AS TEXTFILE");
spark.sql("LOAD DATA LOCAL INPATH 'C:/eclipse/workspace/spark/src/main/resources/hive_data_test.csv' OVERWRITE INTO TABLE csv_test");
spark.table("csv_test").show();
spark.table("csv_test").select('name').show()
spark.table("csv_test").filter('age <=3').select('name').show()
spark.sql("SELECT * FROM csv_test where age<=3").explain()

students=spark.createDataFrame([{'student_id':'1','s_name':'Ravi','dep_id':'10'},{'student_id':'2','s_name':'Sankar','dep_id':'20'},{'student_id':'3','s_name':'Kumar','dep_id':'30'}])
students.show()
departments=spark.createDataFrame([{'depart_id':'10','dep_name':'ECE','location':'Nellore'},{'depart_id':'20','dep_name':'EEE','location':'Ongole'},{'depart_id':'40','dep_name':'CSE','location':'Kadapa'}])
departments.show()

students.join(departments, students.dep_id == departments.depart_id, "inner").show();
students.join(departments, students['dep_id'] == departments['depart_id'], "inner").show();
students.join(departments, students.dep_id == departments.depart_id, "left").show();
students.join(departments, students.dep_id == departments.depart_id, "left").where('depart_id is null').show();
students.join(departments, students.dep_id == departments.depart_id, "right").show();
students.join(departments, students.dep_id == departments.depart_id, "right").where('dep_id is null').show();
students.join(departments, students.dep_id == departments.depart_id, "full").show();
students.join(departments, students.dep_id == departments.depart_id, "full").where('dep_id is null or depart_id is null').show();


namespace = 'NAMESPACE'
catalog = ''.join("""{
"table":{"namespace":"%s", "name":"TABLE_NMAE", "tableCoder":"Phoenix", "version":"2.0"},
"rowkey":"key",
"columns":{
"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
"person_id":{"cf":"r", "col":"id", "type":"string"},
"person_name":{"cf":"r", "col":"name", "type":"string"},
"person_age":{"cf":"r", "col":"age", "type":"string"}
}
}""".split()) % (namespace.upper())

df = spark.read \
    .options(catalog=catalog) \
    .option("hbase.zookeeper.quorum", "localhost:2181") \
    .format("org.apache.spark.sql.execution.datasources.hbase") \
    .load()
df.printSchema()
df.count()
df.createOrReplaceTempView("temp_view")
sparl.sql("select person_name, person_age from temp_view")

df1.repartition(1).write.format('orc').mode('append').save("hdfs://localhost:9000/tmp/MY_TABLE__SOME_NAME/year=2020/month=07/day=31/hour=00/")
or
df1.repartition(1).write.format('orc').mode('append').save("/tmp/MY_TABLE__SOME_NAME/year=2020/month=07/day=31/hour=00/")
spark.sql("msck repair table NAMESPACE.TABLE_NMAE")

```

show partitions HIVE_TABLE;
describe formatted HIVE_TABLE;
hive> LOAD DATA INPATH '/user/haduser/test_table.csv' INTO TABLE test_table;'
./metatool -listFSRoot

Correcting the NameNode location:
$ /usr/lib/hive/bin/metatool -updateLocation hdfs://hadoop:8020 hdfs://localhost:8020        