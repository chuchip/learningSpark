package com.profesorp.learn.ml;



import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.classification.LogisticRegression;
import java.util.Scanner;

public class LearnML {

    SparkSession spark;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Learn ML")
                .master("local[*]")
                .getOrCreate();
        new LearnML(spark);

        System.out.println("Press <ENTER> to finalize the program. Port 4040 to see spark UI");
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
    public LearnML(SparkSession spark) {
        this.spark = spark;

        caseGym(readCsvGym());
    }

    private Dataset<Row>  readCsvGym()
    {
        return spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/ml/GymCompetition.csv");
    }
    private void caseGym(Dataset<Row> ds)
    {
        VectorAssembler vectorAssembler=new VectorAssembler();
        vectorAssembler.setInputCols(new String[] {"Age","Height","Weight"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(ds);

        Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps","features")
                .withColumnRenamed("NoOfReps", "label");
        modelInputData.show();

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(modelInputData);
        System.out.println("The model has intercept " + model.intercept() + " and coefficients " + model.coefficients());

        model.transform(modelInputData).show();
    }
    private Dataset<Row>  readCsvHouse()
    {
       return spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/ml/kc_house_data.ml");
    }
}
