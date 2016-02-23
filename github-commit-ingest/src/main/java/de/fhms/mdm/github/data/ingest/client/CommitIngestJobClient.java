package de.fhms.mdm.github.data.ingest.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import scala.reflect.internal.pickling.UnPickler;


/**
 * Created by Dave on 04.02.2016.
 */
public class CommitIngestJobClient {
    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";
    private static final String PROXY = "10.60.17.102";

    public static void main(String[] args) throws Exception{
        //Proxy setzen
        System.setProperty("http.proxyHost",PROXY);
        System.setProperty("http.proxyPort","8080");
        System.setProperty("https.proxyHost",PROXY);
        System.setProperty("https.proxyPort","8080");

        Configuration conf = new Configuration();
        conf.set(HBASE_MASTER,HBASE_MASTER_HOST);
        Job job = Job.getInstance(conf,"GithubFetcher");

        job.setJarByClass(CommitIngestJobClient.class);
        job.setMapperClass(GithubCommitFetcher.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("repos"));


        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("Repositories".getBytes()),        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                GithubCommitFetcher.class,   // mapper
                RepositoryWritable.class, // mapper output key
                Text.class, // mapper output value
                job);

        job.setReducerClass(GithubDataFlumeSender.class);
        FileOutputFormat.setOutputPath(job,new Path(args[0]));



        System.exit(job.waitForCompletion(true)? 0 : 1);

    }

}
