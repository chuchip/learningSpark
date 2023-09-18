package com.profesorp.learn.rdd;



import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LearnRddApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    LearnRddApp app =
        new LearnRddApp();
    SparkConf conf = new SparkConf().setAppName("PrintJavaRDDValues").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
   // app.groups(sc);
    //app.flatMap(sc);
    //app.readFile(sc);
    //app.relevantWords(sc);
    app.joins(sc);
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
            .appName("Record transformations")
            .master("local")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//    // Create a list of numbers
    List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5);

// Create a Dataset from the list
    Dataset<Integer> dataset = spark.createDataset(integerList, Encoders.INT());
    JavaRDD<Integer> rdd = dataset.toJavaRDD();
    Tuple3<Integer,Integer,Integer>t=new Tuple3<>(1,2,3);
    dataset.show();
   // rdd.foreach(value -> System.out.println(value));
    Integer f=rdd.reduce( (a,b) -> a+b);
    System.out.println("Reduce is: "+f);
    JavaRDD<Double> val=rdd.map(d-> d*1.10);
    val.foreach(value -> System.out.println(value));
    //rdd.toDF();
    //var rdd =
//    System.out.println("*** Looking at partitions");
//    System.out.println("Partition count after repartition: " +
//        df.rdd().partitions().length);
    spark.close();
  }
  private void start1()
  {
    // Create a SparkConf and JavaSparkContext
    SparkConf conf = new SparkConf().setAppName("PrintJavaRDDValues").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a JavaRDD (Replace this with your actual JavaRDD)
    JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("Value1", "Value2", "Value3"));

    // Use the foreach method to print each element in the RDD
    javaRDD.foreach(value -> System.out.println(value));

    // Remember to stop the SparkContext when done
    sc.stop();
    sc.close();
  }
  private void groups(JavaSparkContext sc )
  {

    List<String> logs=Arrays.asList("WARN: warning 1","WARN: warning 2","WARN: warning 3",
            "ERROR: Error 1","ERROR: err 2",
            "INFO: INFO1");
    JavaRDD<String> javaRDD = sc.parallelize(logs);

    /*JavaPairRDD<String, String> rddGroup= javaRDD.mapToPair(d ->
    {
      String data[]=d.split(":");
      return new Tuple2(data[0],data[1] );
    });
    List<Tuple2<String, Iterable<String>>> rdd1=rddGroup.groupByKey().collect();
    rdd1.forEach( (t) -> System.out.println("Key: "+t._1+ " value: "+ Iterables.size(t._2)));
     */

     javaRDD.mapToPair( d-> new Tuple2<String,Integer>(d.split(":")[0],1) )
            .reduceByKey((a,b) -> a+b)
            .foreach( t -> System.out.println("Key: "+t._1+ " value: "+ t._2) );
  }
  private void flatMap(JavaSparkContext sc )
  {
    List<String> logs=Arrays.asList("WARN: warning 1","","WARN: warning 3",
            "ERROR: Error 1","ERROR: err 2",
            "INFO: INFO1");

    JavaRDD<String> javaRDD = sc.parallelize(logs);
    System.out.println("Number partitions: "+javaRDD.getNumPartitions()+ " size array: "+logs.size());
    javaRDD= javaRDD.repartition(1);
    System.out.println("Number partitions: "+javaRDD.getNumPartitions()+ " size array: "+logs.size());
    JavaRDD<String> rddWords=javaRDD.flatMap(s -> Iterators.forArray(s.split(" ")));
    rddWords.foreach( t -> System.out.print(" "+t) );
  }
  private void readFile(JavaSparkContext sc )
  {

    JavaRDD<String> fileRDD =sc.textFile(//
            // System.getProperty("user.dir")+
            "src/main/resources/log4j2.properties");
    LongAccumulator accum = sc.sc().longAccumulator();
    fileRDD.foreach( t -> {
      accum.add(1L);
      System.out.println(" "+t); });
    System.out.println(" Numero lineas: "+accum.value());
  }

  private void relevantWords(JavaSparkContext sc )
  {
    LongAccumulator accum = sc.sc().longAccumulator();
    JavaRDD<String> inputRDD =sc.textFile("src/main/resources/subtitles/input*");
    JavaRDD<String> boringRDD =sc.textFile("src/main/resources/subtitles/boring*");


    JavaPairRDD<String,Integer> wordsRDD= inputRDD
            .filter(s -> s.replaceAll("[^a-zA-Z^]","").trim().length()>0)
            .flatMap(s -> Iterators.forArray(s.split(" ")))
            .filter( s -> s.length()>3 &&  !s.equals("-->") && !s.contains("'")) //&& Character.isUpperCase(s.charAt(0)) )
            .map(s ->
                    (s.endsWith(".") || s.endsWith(",")?s.substring(0,s.length()-1):s).toLowerCase()
            )
            .mapToPair(s ->
            {
              accum.add(1);
              return new Tuple2<String, Integer>(s, 1);
            })
            .reduceByKey((a,b) -> a+b);
    System.out.println("Found words: "+wordsRDD.count());
    wordsRDD = wordsRDD.subtractByKey(boringRDD.mapToPair(w -> new Tuple2<String, Integer>(w,1)));
    System.out.println("Count after remove boring words: "+wordsRDD.count());
    wordsRDD.takeOrdered(10, new TupleComparator())
            .forEach(t -> System.out.println("Word: "+ t._1 + " times: "+ t._2));


    System.out.println(" Read: "+accum.value() +" words ");
  }
  private void joins(JavaSparkContext sc )
  {
    List<Tuple2<Integer,String>> names=Arrays.asList(new Tuple2(1,"Chuchi"),
            new Tuple2(2,"Jose"),
            new Tuple2(3,"Luis"),
            new Tuple2(4,"Maria"),
            new Tuple2(5,"Lucia"));
    List<Tuple2<Integer,Integer>> edades=Arrays.asList(new Tuple2(1,22),
            new Tuple2(2,33),
            new Tuple2(5,25),
            new Tuple2(15,25)
            );

    JavaPairRDD<Integer,String> nombresRDD = sc.parallelizePairs(names);
    JavaPairRDD<Integer,Integer> edadesRDD = sc.parallelizePairs(edades);
    JavaPairRDD<Integer, Tuple2<String, Integer>> joinRdd = nombresRDD.join(edadesRDD);
    System.out.println("---- printing Join ---");
    joinRdd.take(50).forEach(System.out::println);

    System.out.println("\n---- printing Left Join ---");
    JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftJoinRdd = nombresRDD.leftOuterJoin(edadesRDD);
    leftJoinRdd.take(50).forEach(System.out::println);
    System.out.println();
    System.out.println("\n---- printing right Join ---");
    JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightJoinRdd = nombresRDD.rightOuterJoin(edadesRDD);
    rightJoinRdd.take(50).forEach(System.out::println);

    System.out.println("\n---- printing Full Join ---");
    JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> fullJoinRdd = nombresRDD.fullOuterJoin(edadesRDD);
    fullJoinRdd.take(50).forEach(System.out::println);

  }
  static class TupleComparator implements Comparator<Tuple2<String, Integer>>, java.io.Serializable {
    @Override
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return Integer.compare(t2._2(), t1._2());
    }
  }

}
