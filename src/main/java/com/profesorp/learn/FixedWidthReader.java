package com.profesorp.learn;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedWidthReader implements Serializable {

    AtomicInteger atomicInteger= new AtomicInteger();
    final SparkSession spark;
    String fileInput = "./src/main/resources/fixedfile.txt";
    String outputFile = "./src/main/resources/output";
    final JavaSparkContext sc;
    int recordSize = 70; // Adjust this based on your actual record size
    final StructType schema;
    public static void main(String[] args) {



        FixedWidthReader fixedWidthReader = new FixedWidthReader();

    }
    public FixedWidthReader()
    {
        spark = SparkSession
                .builder()
                .appName("FixedWidthFileRead")
                .master("local[*]")
                .getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        schema = new StructType()
                .add(StructField.apply("customerID", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("name", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("city", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("state", DataTypes.StringType, true, Metadata.empty()));
      //  createFile(fileInput);
        prueba3();
    }
    void prueba3()
    {
        JavaRDD<byte[]> records = sc.binaryRecords(fileInput, recordSize);

        JavaRDD<Row> recordsRDD = records.map(split ->
        {
           String record= new String(split, StandardCharsets.UTF_8);
            return RowFactory.create(record.substring(0, 10),
                    record.substring(10, 30),
                    record.substring(30, 50),
                    record.substring(50, 70));
        });

        writeFile(recordsRDD);

    }
    void prueba2()
    {
        // Read the file as an RDD of lines
        JavaRDD<String> linesRDD = sc.textFile(fileInput);

        // Split each line into records based on the fixed size
        JavaRDD<Row> recordsRDD = linesRDD.flatMap(line -> {
            List<Row> records = new ArrayList<>();
            for (int i = 0; i < line.length(); i += recordSize) {
                int endIndex = Math.min(i + recordSize, line.length());
//                if (i > 50*recordSize)
//                    break;
                String linea=line.substring(i, endIndex);
                records.add( RowFactory.create(linea.substring(0, 10),
                        linea.substring(10, 30),
                        linea.substring(30, 50),
                        linea.substring(50, 70)));
            }
            return records.iterator();
        });
        writeFile(recordsRDD);

        // Now 'recordsRDD' contains all the records from the file

    }
    void writeFile(JavaRDD<Row> recordsRDD)
    {
        Dataset<Row> df = spark.createDataFrame(recordsRDD,schema);
        df.write()
                .option("header", true)  // Include a header row
                .mode("overwrite")       // Overwrite any existing file
                .csv(outputFile);
    }
    void prueba1()
    {
        // ... (Same schema definition as before) ...
        StructType schema = new StructType()
                .add(StructField.apply("customerID", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("name", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("city", DataTypes.StringType, true, Metadata.empty()))
                .add(StructField.apply("state", DataTypes.StringType, true, Metadata.empty()));

        // Read the fixed-width file
        Dataset<Row> df = spark
                .read()
                .option("wholeFile", true)
                .format("text")
              //  .schema(schema)
                .load(fileInput);
        df = df.selectExpr("value", "explode(split(content, '(?<=\\G.{" + recordSize + "})')) as record");


//                    .withColumn("customerID", col("value").substr(0, 3))
//                .withColumn("name", col("value").substr(3, 14))
//                .withColumn("city", col("value").substr(17, 13));
        df.show();
        Dataset<Row> modifiedDf = df.map((Function1<Row, Row>) (Row row) -> {
            String customerID = row.getAs("customerID").toString().trim();
            String name = row.getAs("name").toString().trim().toUpperCase();
            String city = row.getAs("city").toString();
            String state = row.getAs("state").toString() + "-US";

            // Create a new Row with modified values
            return RowFactory.create(customerID, name, city, state);
        },  RowEncoder.apply(schema)); // Important for maintaining the schema
        df.show();
        // Process and save as CSV
        df.selectExpr("concat_ws(',', customerID, name, city, state)")
                .write()
                .option("header", true) // Add header row
                .mode("overwrite")
                .csv("./output.csv");

        spark.stop();
    }
    static void createFile(String nameFile)
    {

        try (FileWriter writer = new FileWriter(nameFile)) {
            for (int i = 0; i < 9000000; i++) {
                String line = String.format("%10s%20s%20s%20s",
                        i,".f1-"+i,".f2-"+i,".f3-"+i+".");
                writer.write(line);
            }
        } catch (IOException e) {
            System.out.println("Error writing file: " + e.getMessage());
        }

        System.out.println("Example file 'sample_fixedwidth.txt' generated.");
    }

}
