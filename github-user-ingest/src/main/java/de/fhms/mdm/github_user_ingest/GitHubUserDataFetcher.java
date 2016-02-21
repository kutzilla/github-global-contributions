package de.fhms.mdm.github_user_ingest;

import de.fhms.mdm.github_user_ingest.service.GitHubUserService;
import de.fhms.mdm.github_user_ingest.service.GithubUser;
import org.apache.commons.io.input.NullInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Dave on 21.02.2016.
 */
public class GitHubUserDataFetcher {
    private static final String BASE_PATH = "raw";
    private static final String HDFS_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera";
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";
    private static final String CLIENT_USER = "schleusenfrosch";

    /*
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",HDFS_PATH);

        Job job = Job.getInstance(conf, "githubuserdatafetcher");
        job.setJarByClass(GitHubUserDataFetcher.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] owners = fileSystem.listStatus(new Path("/raw"));


        if(owners.length>1) {
            for (FileStatus file : owners) {
                System.out.println(file.getPath().getName());
            }
        }



        for (FileSystem repo : owners) {
            for (FileSystem committer:repo.getChildFileSystems()) {
                System.out.println(committer.getName());
            }
        }
        GithubUser user = new GithubUser(API_TOKEN,CLIENT_USER);
        GitHubUserService gitHubUserService = new GitHubUserService(user);
    }
    */
}
