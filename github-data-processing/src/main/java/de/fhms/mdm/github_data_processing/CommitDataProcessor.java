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
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;

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

    private static Function filter = (Function<String, Boolean>) new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            try {
                JSONObject jsonObject = new JSONObject(s);
                JSONObject obj = (JSONObject) jsonObject.get("author");
                return obj != null;
            } catch (Exception e) {
                return false;
            }
        }
    };



    private static Function map = (Function<String, User>) new Function<String, User>() {
        @Override
        public User call(String s) throws Exception {
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
        }
    };

    private static VoidFunction<User> forEach = (VoidFunction<User>) new VoidFunction<User>() {
        @Override
        public void call(User user) throws Exception {
            System.out.println(user);
        }
    };

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
                new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Location>() {
                    @Override
                    public Iterable<Tuple2<String, Location>> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                        String s = new String(t._1().get());
                        Result result = t._2();
                        byte[] longitudeBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                                Bytes.toBytes(COLUMN_LONGITUDE));
                        byte[] latitudeBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                                Bytes.toBytes(COLUMN_LATITUDE));
                        byte[] cityBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                                Bytes.toBytes("city"));
                        byte[] countryBytes = result.getValue(Bytes.toBytes(LOCATION_COLUMN_FAMILY),
                                Bytes.toBytes("country"));
                        Location location = new Location();
                        location.setLongitude(new String(longitudeBytes));
                        location.setLatitude(new String(latitudeBytes));
                        location.setCity(new String(cityBytes));
                        location.setCountry(new String(countryBytes));
                        return Arrays.asList(new Tuple2<>(s, location));
                    }
                });


        JavaPairRDD<ImmutableBytesWritable, String> formatedUsers = users
                .mapValues(new Function<Result, String>() {
                    @Override
                    public String call(Result r) throws Exception {
                        byte[] locationBytes = r.getValue(Bytes.toBytes(USER_COLUMN_FAMILY),
                                Bytes.toBytes(LOCATION_COLUMN_FAMILY));
                        return new String(locationBytes);
                    }
                });

        JavaPairRDD<String, String> reformatedUsers = formatedUsers
                .flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, String>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<ImmutableBytesWritable, String> t) throws Exception {
                        String s = new String(t._1().get());
                        return Arrays.asList(new Tuple2<>(t._2(), s));
                    }
                });

        JavaPairRDD<String, Tuple2<String, Location>> localizedUsers = reformatedUsers.join(formattedLocations);



        JavaPairRDD<String, Location> userRdd = jsc.parallelizePairs(localizedUsers.values().collect());

        JavaRDD<String> textFile = cdp.getJavaSparkContext().textFile("hdfs://quickstart.cloudera:8020/user/cloudera/data/repos/processing/*.dat");

        JavaPairRDD<String, String> userMappedCommits = jsc.parallelizePairs(textFile.map(new Function<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                JSONObject committer = (JSONObject) jsonObject.get("committer");
                String login = (String) committer.get("login");
                return new Tuple2<>(login, s);
            }
        }).collect());


        JavaPairRDD<String, Tuple2<Location, String>> commits = userRdd.join(userMappedCommits);

        commits.filter(new Function<Tuple2<String, Tuple2<Location, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Location, String>> v) throws Exception {
                Location loc = v._2()._1();
                return loc.getLongitude() != null && loc.getLatitude() != null;
            }
        });

        commits.foreach(new VoidFunction<Tuple2<String, Tuple2<Location, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Location, String>> pair) throws Exception {
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
                put.add(Bytes.toBytes("user"), Bytes.toBytes("email"), Bytes.toBytes(getEmail(json)));
                put.add(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes(loc.getCity()));
                put.add(Bytes.toBytes("location"), Bytes.toBytes("country"), Bytes.toBytes(loc.getCountry()));
                put.add(Bytes.toBytes("location"), Bytes.toBytes("longitude"), Bytes.toBytes(loc.getLongitude()));
                put.add(Bytes.toBytes("location"), Bytes.toBytes("latitude"), Bytes.toBytes(loc.getLatitude()));
                put.add(Bytes.toBytes("repo_data"), Bytes.toBytes("owner"), Bytes.toBytes(getOwner(json)));
                put.add(Bytes.toBytes("repo_data"), Bytes.toBytes("name"), Bytes.toBytes(getName(json)));
                put.add(Bytes.toBytes("commit_data"), Bytes.toBytes("message"), Bytes.toBytes(getMessage(json)));
                put.add(Bytes.toBytes("commit_data"), Bytes.toBytes("date"), Bytes.toBytes(getDate(json)));
                commitTable.put(put);
                conn.close();
            }
        });

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

    private static String getEmail(JSONObject json) {
        try {
            JSONObject commit = (JSONObject) json.get("commit");
            JSONObject committer = (JSONObject) commit.get("committer");
            String date = (String) committer.get("date");
            return date;
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
            date = (String) committer.get("date");
            String sfd = "yyyy-MM-dd'T'HH:mm:ss'Z'";
            SimpleDateFormat sdf = new SimpleDateFormat(sfd);

            return sdf.parse(date).getTime() + "";
        } catch (Exception e) {
            e.printStackTrace();
            return "none";
        }
    }
}
