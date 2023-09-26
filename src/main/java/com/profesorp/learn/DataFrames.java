package com.profesorp.learn;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Scanner;

public class DataFrames {
    Dataset<Row> studentsDS;
    SparkSession spark;

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Record transformations")
                .master("local[*]")
                .getOrCreate();
        new DataFrames(spark);
        System.out.println("Press <ENTER> to finalize the program. Port 4040 to see spark UI");
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
    public DataFrames(SparkSession spark) {
        this.spark = spark;
//
     //   filtering();
     //   usingSql();
//        grouping();

//        avgPivotJava();
//        grouping();
//        groupingJava();
        udfs();
    }
    void readStudents()
    {
        studentsDS = spark.read().
                option("header", true).csv("src/main/resources/students.csv");
    }
    void usingSql()
    {
        readStudents();
        studentsDS.createOrReplaceTempView("students_view");
        Dataset<Row> result=spark.sql("select * from students_view");
        result.show();
    }
    void grouping()
    {
        Dataset<Row> logDS=getBigLog();
        logDS.createOrReplaceTempView("logs_view");
        Arrays.stream(logDS.columns()).forEach(System.out::println);
        //logDS.cache();
//        spark.sql("select datetime,level " +
//                "from logs_view order by datetime").show(5);

        Dataset<Row> group =spark.sql("select date_format(datetime,'yy-MMMM') as month,level,cast(count(*) as int) as count " +
                "from logs_view group by month,level order by level,month");
        group.show(false);
    }


    void filtering()
    {
        readStudents();
        Dataset<Row>  student1=studentsDS.filter(new studentIDFilter());
        student1.show();
        System.out.println("Procesando con lambda ....");
        Dataset<Row>  student2=studentsDS.filter((FilterFunction<Row>) row -> row.getAs("student_id").equals("1"));
        student2.show();
        System.out.println("Procesando con Column ....");
        //Column studentCol= studentsDS.col("student_id") ;
        Column studentCol= org.apache.spark.sql.functions.column("student_id");
        Dataset<Row>  student3=studentsDS.filter( studentCol.equalTo("2"));
        student3.show();
        //studentsDS.filter("student_id=1").show();
   //     studentsDS.filter( r -> r.getAs("student_id").equals("1") );
    }
    void groupingJava()
    {
        Dataset<Row> logDS=getBigLog();
        logDS= logDS.select(
                date_format(column("datetime"),"yyyyMM").as("yearAndMonth"),
                column("level")).groupBy("yearAndMonth","level").count()
                .as("count");
        logDS.show();
        //logDS.cache();
        logDS= logDS.
                select(concat(substring(column("yearAndMonth"),0,4),lit("-"),
                        substring(column("yearAndMonth"),5,2)).as("date"),
                        column("level"),column("count"))
                .orderBy(column("yearAndMonth").cast(DataTypes.IntegerType),column("level") ) ;
        logDS.show();
    }
    void avgPivotJava()
    {
         readStudents();
         System.out.println(" Get the avg of student by subject and year");
         studentsDS.groupBy("subject").pivot("year")
                 .agg( round(avg("score"),2).alias("avg"),
                         round(max("score"),2).alias("max"),
                         round(min("score"),2).alias("min") ).show(true);

        System.out.println(" Get the student with the best score");
         String firstStudent=studentsDS.groupBy("student_id")
                 .agg(round(avg("score"),4).alias("avg_score"))
                 .orderBy(desc("avg_score"))
                 .first().getAs("student_id");
         studentsDS.filter("student_id="+firstStudent).show();
    }
    void udfs()
    {
        readStudents();
        System.out.printf("--- UDF from Datasets ---");
        spark.udf().register("numberToLiteral",(Integer in) -> in==1?"One":"More than one: "+in,DataTypes.StringType);
        studentsDS.select(col("student_id"),
                callUDF("numberToLiteral",col("score").cast(DataTypes.IntegerType)).alias("score_udf"))
                .show();
        studentsDS.createOrReplaceTempView("students_view");
        System.out.printf("--- UDF con SQL ---");
        spark.sql("select student_id,numberToLiteral(cast(score as int)) as score_udf from students_view").show();
    }

    Dataset<Row>  getBigLog() {
        return spark.read().option("header", true)
                .option("delimiter", ",")
                .csv("src/main/resources/biglog.txt");
    }
    final static class studentIDFilter implements FilterFunction<Row>, Serializable {
        private static final long serialVersionUID = 17392L;

        public studentIDFilter()
        {
            System.out.println("Construyendo clase studentIDFilter");
        }
        @Override
        public boolean call(Row r) {
            //System.out.println("Call "+r.getAs("subject").toString());
            if (r.getAs("student_id" ).equals("1")) {
                return true;
            }
            return false;
        }
    }

}

