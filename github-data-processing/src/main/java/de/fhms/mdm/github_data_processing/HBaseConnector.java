package de.fhms.mdm.github_data_processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by Matthias on 23.02.16.
 */
public class HBaseConnector {

    // Spark Konfigurationen
    private static final String SPARK_MASTER = "local";
    private static final String SPARK_APP_NAME = HBaseConnector.class.getSimpleName();

    // HBase Konfigurationen
    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";

    // Hadoop Konfigurationen
    private static final String USER_NAME_PROPERTY = "user.name";
    private static final String HADOOP_USER_NAME_PROPERTY = "HADOOP_USER_NAME";
    private static final String HDFS_PROPERTY_VALUE = "hdfs";


    // HBase Tabellen
    private static final String LOCATIONS_TABLE_NAME = "Locations";

    public static void main(String[] args) {

        // Hadoop Konfigurationen
        System.setProperty(USER_NAME_PROPERTY, HDFS_PROPERTY_VALUE);
        System.setProperty(HADOOP_USER_NAME_PROPERTY, HDFS_PROPERTY_VALUE);

        // Spark Konfiguration
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(SPARK_MASTER);
        sparkConf.setAppName(SPARK_APP_NAME);

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // HBase Konfiguration
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HBASE_MASTER, HBASE_MASTER_HOST);
        hbaseConf.set(TableInputFormat.INPUT_TABLE, LOCATIONS_TABLE_NAME);

        JavaPairRDD<ImmutableBytesWritable, Result> locations =
                jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        System.out.println("Die Location Anzahl umfasst: " + locations.count());

        locations.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> pair) throws Exception {
                String key = new String(pair._1().get());
                System.out.println(key);
            }
        });

    }
}
