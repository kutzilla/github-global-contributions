package de.fhms.mdm.github_data_processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Created by Matthias on 19.02.16.
 */
public class CommitDataProcessor implements Serializable {

    private static final String MASTER = "local";

    private static final String APP_NAME = "CommitDataProcessor";

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

    public JavaRDD<Commit> processCommitData(JavaRDD<String> inputFile) {
        JavaRDD<Commit> commits = inputFile.map((Function<String, Commit>) s -> {
            JSONObject object = new JSONObject(s);
            JSONObject commitJson = (JSONObject) object.get("commit");
            String message = (String) commitJson.get("message");
            Commit commit = new Commit();
            commit.setMessage(message);
            return commit;
        });
        commits.foreach((VoidFunction<Commit>) commit -> {
            System.out.println(commit);
        });
        return commits;
    }


    public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }



    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("input file as parameter is mandatory");
            System.exit(-1);
        }
        SparkConf sparkConf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
        CommitDataProcessor cdp = new CommitDataProcessor();
        cdp.setJavaSparkContext(new JavaSparkContext(sparkConf));
        cdp.processUserData(args[0]);
        JavaRDD<String> textFile = cdp.getJavaSparkContext().textFile(args[0]);
        cdp.processCommitData(textFile);
    }
}
