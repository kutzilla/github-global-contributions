package de.fhms.mdm.github_data_ingest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by Matthias on 03.02.16.
 */
public class PushEventTransformer {

    private static final String SPARK_MASTER = "local";

    private static final String APP_NAME = "github-data-processing";

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];


        SparkConf conf = new SparkConf().setMaster(SPARK_MASTER)
                .setAppName(APP_NAME);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> input = sparkContext.textFile(inputFile);

        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                String[] values = s.split(";");
                String sha = values[0];
                String message = values[1];
                String commitDate = values[2];
                String commiter = values[3];
                String email = values[4];
                String location = values[5];
                String pushDate = values[6];
                return Arrays.asList(location);
            }
        });

        words.saveAsTextFile(outputFile);
    }
}
