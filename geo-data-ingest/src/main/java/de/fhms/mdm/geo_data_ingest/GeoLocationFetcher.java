package de.fhms.mdm.geo_data_ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationFetcher {

    public static final String PROXY = "10.60.17.102";

    public static final String LOCATIONS_TABLE = "Locations";
    public static final String LOCATION_COLUMN_FAMILY = "location";

    private static final String JOB_NAME = GeoLocationFetcher.class.getSimpleName().toLowerCase();

    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_MASTER, HBASE_MASTER_HOST);
        Connection connection = ConnectionFactory.createConnection(conf);

        Table locationsTable = connection.getTable(TableName.valueOf(LOCATIONS_TABLE));

        Scan locationScan = new Scan().setCaching(500).setCacheBlocks(false)
                .addFamily(Bytes.toBytes(LOCATION_COLUMN_FAMILY));

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(GeoLocationFetcher.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        TableMapReduceUtil.initTableMapperJob(locationsTable.getName(), locationScan,
                GeoLocationTableMapper.class, ImmutableBytesWritable.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob(new String(locationsTable.getName().getName()), null, job);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true)? 0 : 1);

    }
}
