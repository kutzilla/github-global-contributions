package de.fhms.mdm.github_data_processing;

import org.apache.commons.logging.Log;
import org.apache.log4j.Logger;
import org.apache.spark.Logging;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

/**
 * Created by Matthias on 19.02.16.
 */
public class CommitDataProcessor {

    private static final String MASTER = "local";

    private static final String APP_NAME = "CommitDataProcessor";

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("input file as parameter is mandatory");
            System.exit(-1);
        }

        SparkConf sparkConf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sc.textFile(args[0]);

        System.out.println("Input File Line Count " + inputFile.count());

        JavaRDD<String> registeredCommits = inputFile.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                try {
                    JSONObject obj = (JSONObject) jsonObject.get("author");
                    return true;
                } catch (ClassCastException e) {
                    return false;
                }
            }
        });

        System.out.println("Registered Commits Line Count " + registeredCommits.count());

        JavaRDD<User> users = registeredCommits.map(new Function<String, User>() {
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
        });

        users.foreach(new VoidFunction<User>() {
            public void call(User user) throws Exception {
                System.out.println(user);
            }
        });
    }
}
