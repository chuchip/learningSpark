package com.profesorp.learn.ml;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Scanner;
import static org.apache.spark.sql.functions.*;
public class VppChapterView {
    SparkSession spark;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Chapter View")
                .master("local[*]")
                .getOrCreate();
        new VppChapterView(spark);

        System.out.println("Press <ENTER> to finalize the program. Port 4040 to see spark UI");
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
    public VppChapterView(SparkSession spark) {
        this.spark = spark;
        Dataset<Row> dsInput=readCsv();
      //  dsInput.printSchema();
        dsInput=dsInput.filter("is_cancelled = false").drop("observation_date","is_cancelled");
        for (String col: dsInput.columns())
        {
            boolean isNull=! dsInput.filter(col+" is null").isEmpty();
            if (isNull) {
                System.out.println("Col: " + col + " had null values: ");
                dsInput = dsInput.withColumn(col+"not_null",when(col(col).isNull(),0).otherwise(col(col)))
                        .drop(col).withColumnRenamed(col+"not_null",col);
            }
        }
        dsInput = dsInput.withColumnRenamed("next_month_views", "label");
        StringIndexer paymentIndexer= new StringIndexer();
        paymentIndexer.setInputCols(new String[]{"payment_method_type","country","rebill_period_in_months"});
        paymentIndexer.setOutputCols(new String[]{"paymentIndexer","countryIndexer","rebillIndexer"});
        dsInput=paymentIndexer.fit(dsInput).transform(dsInput);

        OneHotEncoder oneHotEncoder=new OneHotEncoder();
        oneHotEncoder.setInputCols(new String[]{"paymentIndexer","countryIndexer","rebillIndexer"});
        oneHotEncoder.setOutputCols(new String[]{"paymentEncoder","countryEncoder","rebillEncoder"});
        dsInput=oneHotEncoder.fit(dsInput).transform(dsInput);

        VectorAssembler vectorAssembler = new VectorAssembler();
        Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"firstSub","age","all_time_views","last_month_views",
                        "paymentEncoder","countryEncoder","rebillEncoder"})
                .setOutputCol("features")
                .transform(dsInput).select("label","features");

        Dataset<Row>[] dataSplits = inputData.randomSplit(new double[] {0.9, 0.1});
        Dataset<Row> trainingAndTestData = dataSplits[0];
        Dataset<Row> holdOutData = dataSplits[1];

        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01,0.1,0.3,0.5,0.7,1})
                .addGrid(linearRegression.elasticNetParam(),new double[] {0,0.5,1})
                .build();

        TrainValidationSplit tvs = new TrainValidationSplit();
        tvs.setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMap)
                .setTrainRatio(0.9);
        TrainValidationSplitModel model = tvs.fit(trainingAndTestData);

        LinearRegressionModel lrModel = (LinearRegressionModel)model.bestModel();
        System.out.println("The R2 value is " + lrModel.summary().r2());

        System.out.println("coefficients : " + lrModel.coefficients() + " intercept : " + lrModel.intercept());
        System.out.println("reg param : " + lrModel.getRegParam() + " elastic net param : " + lrModel.getElasticNetParam());

        lrModel.transform(holdOutData).show();
    }
    private Dataset<Row> readCsv()
    {
        return spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/ml/vppChapterViews");
    }
}
