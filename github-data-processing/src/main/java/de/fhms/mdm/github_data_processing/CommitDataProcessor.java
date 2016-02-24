package de.fhms.mdm.github_data_processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple1;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Matthias on 19.02.16.
 */
public class CommitDataProcessor implements Serializable {

    private static final String COLUMN_LONGITUDE = "longitude";

    private static final String COLUMN_LATITUDE = "latitude";

    private static final String LOCATION_TABLE_NAME = "Locations";
    private static final String USERS_TABLE_NAME = "Users";

    private static final String USER_COLUMN_FAMILY = "user";
    public static final String LOCATION_COLUMN_FAMILY = "location";

    private static final String MASTER = "local";

    private static final String APP_NAME = "CommitDataProcessor";

    // HBase Konfigurationen
    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";

    // Hadoop Konfigurationen
    private static final String USER_NAME_PROPERTY = "user.name";
    private static final String HADOOP_USER_NAME_PROPERTY = "HADOOP_USER_NAME";
    private static final String HDFS_PROPERTY_VALUE = "hdfs";

    // HBase Tabellen
    private static final String LOCATIONS_TABLE_NAME = "Locations";

    private JavaSparkContext javaSparkContext;

    private static Function filter = (Function<String, Boolean>) s -> {
        try {
            JSONObject jsonObject = new JSONObject(s);
            JSONObject obj = (JSONObject) jsonObject.get("author");
            return obj != null;
        } catch (Exception e) {
            return false;
        }
    };



    private static Function map = (Function<String, User>) s -> {
        User user = new User();
        JSONObject jsonObject = new JSONObject(s);
        JSONObject committer = (JSONObject) jsonObject.get("committer");
        String login = (String) committer.get("login");
        user.setLogin(login);
        JSONObject commit = (JSONObject) jsonObject.get("commit");
        JSONObject commitCommiter = (JSONObject) commit.get("committer");
        String email = (String) commitCommiter.get("email");
        user.setEmail(email);
        return user;
    };

    private static VoidFunction<User> forEach = (VoidFunction<User>) user -> System.out.println(user);

    public JavaRDD<User> processUserData(String inputFilePath) {
        JavaRDD<String> inputFile = javaSparkContext.textFile(inputFilePath);

        System.out.println("Input File Line Count " + inputFile.count());

        JavaRDD<String> registeredCommits = inputFile.filter(filter);

        System.out.println("Registered Commits Line Count " + registeredCommits.count());

        JavaRDD<User> users = registeredCommits.map(map);

        users.foreach(forEach);

        return users;
    }

    public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }



    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("input file as parameter is mandatory");
            System.exit(-1);
        }

        SparkConf sparkConf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
        CommitDataProcessor cdp = new CommitDataProcessor();
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        cdp.setJavaSparkContext(jsc);
        //cdp.processUserData(args[0]);


        Configuration hbaseLocationsConf = HBaseConfiguration.create();
        hbaseLocationsConf.set(HBASE_MASTER, HBASE_MASTER_HOST);
        hbaseLocationsConf.set(TableInputFormat.INPUT_TABLE, LOCATIONS_TABLE_NAME);

        // HBase User Konfiguration
        Configuration hbaseUsersConf = HBaseConfiguration.create();
        hbaseUsersConf.set(HBASE_MASTER, HBASE_MASTER_HOST);
        hbaseUsersConf.set(TableInputFormat.INPUT_TABLE, USERS_TABLE_NAME);

        JavaPairRDD<ImmutableBytesWritable, Result> users =
                jsc.newAPIHadoopRDD(hbaseUsersConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);


        JavaPairRDD<ImmutableBytesWritable, Result> locations =
                jsc.newAPIHadoopRDD(hbaseLocationsConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, Location> formattedLocations = locations.flatMapToPair(
                (PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Location>) t -> {
            String s = new String(t._1().get());
                    Result result = t._2();
                    byte[] longitudeBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                            Bytes.toBytes(COLUMN_LONGITUDE));
                    byte[] latitudeBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                            Bytes.toBytes(COLUMN_LATITUDE));
                    Location location = new Location();
                    location.setCity(s);
                    location.setLongitude(new String(longitudeBytes));
                    location.setLatitude(new String(latitudeBytes));
            return Arrays.asList(new Tuple2<>(s, location));
        });


        JavaPairRDD<ImmutableBytesWritable, String> formatedUsers = users
                .mapValues((Function<Result, String>) (r) -> {
            byte[] locationBytes = r.getValue(Bytes.toBytes(USER_COLUMN_FAMILY),
                    Bytes.toBytes(LOCATION_COLUMN_FAMILY));
            return new String(locationBytes);
        });

        JavaPairRDD<String, String> reformatedUsers = formatedUsers
                .flatMapToPair((PairFlatMapFunction<Tuple2<ImmutableBytesWritable, String>, String, String>) t -> {
                String s = new String(t._1().get());
                return Arrays.asList(new Tuple2<>(t._2(), s));
        });

        JavaPairRDD<String, Tuple2<String, Location>> localizedUsers = reformatedUsers.join(formattedLocations);


        localizedUsers.foreach((pair) -> {
            System.out.println(pair._1() + ": " + pair._2()._1() + " - " + pair._2()._2().getLongitude() + ";" + pair._2()._2().getLatitude());
        });

        JavaPairRDD<String, Location> userRdd = jsc.parallelizePairs(localizedUsers.values().collect());

        JavaRDD<String> textFile = cdp.getJavaSparkContext().textFile(args[0]);

        JavaPairRDD<String, String> userMappedCommits = jsc.parallelizePairs(textFile.map((Function<String, Tuple2<String, String>>) s -> {
            JSONObject jsonObject = new JSONObject(s);
            JSONObject committer = (JSONObject) jsonObject.get("committer");
            String login = (String) committer.get("login");
            return new Tuple2<>(login, s);
        }).collect());

        userMappedCommits.foreach((VoidFunction<Tuple2<String, String>>) stringStringTuple2
                -> System.out.println(stringStringTuple2._1() + ": " + stringStringTuple2._2()));

        JavaPairRDD<String, Tuple2<Location, String>> commits = userRdd.join(userMappedCommits);


        commits.foreach(pair -> {
            String login = pair._1();
            Location loc = pair._2()._1();
            String commitJson = pair._2()._2();
            JSONObject json = new JSONObject(commitJson);

            Configuration hbase = HBaseConfiguration.create();
            hbase.set(HBASE_MASTER, HBASE_MASTER_HOST);
            Connection conn = ConnectionFactory.createConnection(hbase);
            Table commitTable = conn.getTable(TableName.valueOf("Commits"));

            Put put = new Put(Bytes.toBytes(getCommitSha(json)));

            put.add(Bytes.toBytes("user"), Bytes.toBytes("login"), Bytes.toBytes(login));
            put.add(Bytes.toBytes("user"), Bytes.toBytes("email"), Bytes.toBytes("none"));
            put.add(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes(loc.getCity()));
            put.add(Bytes.toBytes("location"), Bytes.toBytes("longitude"), Bytes.toBytes(loc.getLongitude()));
            put.add(Bytes.toBytes("location"), Bytes.toBytes("latitude"), Bytes.toBytes(loc.getLatitude()));
            put.add(Bytes.toBytes("repo_data"), Bytes.toBytes("owner"), Bytes.toBytes(getOwner(json)));
            put.add(Bytes.toBytes("repo_data"), Bytes.toBytes("name"), Bytes.toBytes(getName(json)));
            put.add(Bytes.toBytes("commit_data"), Bytes.toBytes("message"), Bytes.toBytes(getMessage(json)));
            put.add(Bytes.toBytes("commit_data"), Bytes.toBytes("date"), Bytes.toBytes(getDate(json)));
            commitTable.put(put);
            conn.close();
        });

        //cdp.processCommitData(textFile);
    }

    private static String getCommitSha(JSONObject json) throws Exception {
        String sha = (String) json.get("sha");
        return sha;
    }

    private static String getMessage(JSONObject json) {
        String message = null;
        try {
            JSONObject commit = (JSONObject) json.get("commit");
            message = (String) commit.get("message");
            return message;
        } catch (JSONException e) {
            e.printStackTrace();
            return "none";
        }
    }

    private static String getOwner(JSONObject json) {
        try {
            String urlString = (String) json.get("url");
            String url = urlString.split("/")[4];
            return url;
        } catch (Exception e) {
            return "none";
        }
    }


    private static String getName(JSONObject json) {
        try {
            String urlString = (String) json.get("url");
            String name = urlString.split("/")[5];
            return name;
        } catch (Exception e) {
            return "none";
        }
    }

    // TODO In Long umwandeln
    private static String getDate(JSONObject json) {
        String date = null;
        try {
            JSONObject commit = (JSONObject) json.get("commit");
            JSONObject committer = (JSONObject) commit.get("committer");
            return (String) committer.get("date");
        } catch (JSONException e) {
            e.printStackTrace();
            return "none";
        }
    }
}
