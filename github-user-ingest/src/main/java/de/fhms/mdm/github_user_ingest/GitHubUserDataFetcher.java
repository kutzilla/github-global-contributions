package de.fhms.mdm.github_user_ingest;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Created by Dave on 21.02.2016.
 */
public class GitHubUserDataFetcher extends Configured implements Tool{
    private static final String HDFS_INPUT_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/raw/users/*.dat";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("gitHubUserDataFetcher");
        job.setJarByClass(GitHubUserDataFetcher.class);
        job.setMapperClass(GitHubUserDataMapper.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path(HDFS_INPUT_PATH));
        job.setNumReduceTasks(0);

        System.out.println("Starting Job###############################################");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args){
        GitHubUserDataFetcher gitHubUserDataFetcher= new GitHubUserDataFetcher();
        try {
            gitHubUserDataFetcher.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
