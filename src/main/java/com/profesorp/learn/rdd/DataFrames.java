package com.profesorp.learn.rdd;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
        grouping();
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
        Dataset<Row> logDS= spark.read().option("header",true)
                .option("delimiter",",")
                .csv("src/main/resources/biglog.txt");
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

