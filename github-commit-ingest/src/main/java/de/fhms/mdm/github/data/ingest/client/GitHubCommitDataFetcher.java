package de.fhms.mdm.github.data.ingest.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by Dave on 04.02.2016.
 */
public class GitHubCommitDataFetcher {
    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set(HBASE_MASTER,HBASE_MASTER_HOST);
        Job job = Job.getInstance(conf);
        job.setJobName("GitHubCommitDataFetcher");

        job.setJarByClass(GitHubCommitDataFetcher.class);
        job.setMapperClass(GitHubCommitDataMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("repos"));


        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("Repositories".getBytes()),        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                GitHubCommitDataMapper.class,   // mapper
                RepositoryWritable.class, // mapper output key
                Text.class, // mapper output value
                job);

        job.setReducerClass(GitHubCommitDataReducer.class);
        /*
        FileSystem fs = FileSystem.get(new Configuration());
        //Output löschen, steht nix drin, wird aber benötigt
        fs.delete(new Path("output"), true);
        */
        FileOutputFormat.setOutputPath(job,new Path("output"));



        System.exit(job.waitForCompletion(true)? 0 : 1);

    }

}
