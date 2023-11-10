package com.profesorp.learn.streaming;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import static org.apache.spark.sql.functions.*;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class LearnStreamKafka {
    SparkSession spark;
    public static void main(String[] args) throws TimeoutException, StreamingQueryException  {
        SparkSession spark = SparkSession.builder()
                .appName("Learn Stream Kafka")
                .master("local[*]")
                .getOrCreate();
        new LearnStreamKafka(spark);

        System.out.println("Press <ENTER> to finalize the program. Port 4040 to see spark UI");
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
    public LearnStreamKafka(SparkSession spark) throws TimeoutException, StreamingQueryException  {
        this.spark = spark;

        simpleRead();
    }
    void simpleRead() throws TimeoutException, StreamingQueryException {
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9093,localhost:9094,localhost:9095")
                .option("subscribe", "viewrecords")
                .load();
        Dataset<Row> results=df.selectExpr(/*"CAST(key AS STRING)",*/ "CAST(value AS STRING) as course_name")
                .groupBy(col("course_name"))
                .agg(count("*").alias("count"));

        StreamingQuery console = results.writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .start();
        console.awaitTermination();
    }
}
