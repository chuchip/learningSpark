        package com.profesorp.learn.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

        public class Figures {
    boolean testMode=false;
    JavaSparkContext sc;
    final JavaPairRDD<Integer,Integer> chapterCourseRDD;  // Chapter - Course
    JavaPairRDD<Integer,Integer> viewsRDD; // Chapter - user
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PrintJavaRDDValues").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Figures app = new Figures(sc);

    }
    Figures(JavaSparkContext sc) {

        this.sc = sc;
        if (testMode)
        {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96,  1));
            rawChapterData.add(new Tuple2<>(97,  1));
            rawChapterData.add(new Tuple2<>(98,  1));
            rawChapterData.add(new Tuple2<>(99,  2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            chapterCourseRDD = sc.parallelizePairs(rawChapterData);
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            viewsRDD=  sc.parallelizePairs(rawViewData).distinct();
        }
        else {
            chapterCourseRDD =
                    sc.textFile("src/main/resources/viewing figures/chapters.csv").mapToPair(
                            d -> {
                                String[] parts = d.split(",");
                                return new Tuple2<Integer, Integer>(Integer.parseInt(parts[0]),
                                        Integer.parseInt(parts[1]));
                            }
                    ); // Chapter - Course
            //titlesRDD.take(5).forEach(System.out::println);
            viewsRDD = sc.textFile("src/main/resources/viewing figures/views-?.csv").mapToPair(
                    d -> {
                        String[] parts = d.split(",");
                        return new Tuple2<Integer, Integer>(Integer.parseInt(parts[0]),
                                Integer.parseInt(parts[1]));
                    }
            ).distinct();
            //System.out.println("Views count: " + viewsRDD.count());
        }
        step2();
        System.out.println("Press <ENTER> to finalize the program. Port 4200 to see spark UI");
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
    }
    void step2()
    {
        JavaPairRDD<Integer, Integer> chaptersByCourse = chapterCourseRDD.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println(" chaptersByCourse --- > ");
        chaptersByCourse.take(5).forEach(System.out::println);

        viewsRDD= viewsRDD.mapToPair(row -> new Tuple2<>(row._2,row._1));
//        JavaPairRDD<Integer, Integer> chaptersByCourseRDD = courseChapterRDD.mapToPair(row -> new Tuple2<Integer, Integer>(row._1, 1)).reduceByKey((a, b) -> a + b);
//        chaptersByCourseRDD.take(5).forEach(System.out::println);
//        //chaptersByCourseRDD.foreach(print::new);
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> viewsWithCourse = viewsRDD.join(chapterCourseRDD); // Chapter, User- Course
        System.out.println(" Join --- > ");
        viewsWithCourse.take(5).forEach(System.out::println);


        // Step 3
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> step3 =
                viewsWithCourse.mapToPair(row -> new Tuple2<Tuple2<Integer, Integer>, Integer>(new Tuple2<Integer, Integer>(row._2._1, row._2._2), 1))
                .reduceByKey((a, b) -> a + b); // User-Course -> Count
        System.out.println(" Step 3 --- > ");
        step3.take(5).forEach(System.out::println);
        // Step 5
        JavaPairRDD<Integer, Integer> step5 = step3.mapToPair(row -> new Tuple2<Integer, Integer>(row._1._2, 1)).reduceByKey((a, b) -> a + b);
        System.out.println(" Step 5 --- > ");
        step5.take(5).forEach(System.out::println);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> step6 = step5.join(chaptersByCourse);
        System.out.println(" Step 6 --- > ");
        step6.take(5).forEach(System.out::println);

        JavaPairRDD<Integer, Float> step7 =
                step6.mapToPair(row -> new Tuple2<Integer,Float>(row._1, (float) row._2._1 / (float)row._2._2));
        System.out.println(" Step 7 --- > ");
        step7.take(5).forEach(System.out::println);

        JavaPairRDD<Integer, Integer> step8 = step7.mapValues(row ->   row > .9 ? 10 : row > .5 ? 4 : row > .25 ? 2 : 0);
        System.out.println(" Step 8 --- > ");
        step8.take(5).forEach(System.out::println);
        JavaPairRDD<String, String> title=sc.parallelizePairs(Arrays.asList(new Tuple2<String ,String >("CourseId","Total")));
//        JavaPairRDD<String, String> step9 = step8.union(title);
//        System.out.println(" Step 9 --- > ");
        title.foreach(print::new);
      //  step8.sortByKey(true).foreach(print::new);
        step8.takeOrdered(3,new IntegerComparator()).forEach(System.out::println);
//        JavaPairRDD<Integer, Integer> chaptersOfCourse = viewsWithCourse.mapToPair(d -> new Tuple2<Integer, Integer>(d._1, d._2._1));
                        //.filter(r -> r._2._1==302 && r._1==177); // Chapter,User,Course
        //courseCountRDD.take(5).forEach(System.out::println);


    }
    static class IntegerComparator implements Comparator<Tuple2<Integer,Integer>> , java.io.Serializable {
        @Override
        public int compare(Tuple2<Integer,Integer> t1, Tuple2<Integer,Integer> t2) {
            return Integer.compare(t2._2,t1._2);
        }
    }
    static class print implements Serializable
    {
        print(Object o)
        {
            System.out.println(o.toString());
        }
    }

}
