package de.fhms.mdm.geo_data_ingest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Count für Städte
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Location auf Basis der Position in den Dateien im HDFS
        try {
            String location = value.toString().split(";")[5];
            context.write(new Text(location), ONE);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(e.getMessage());
        }
    }
}
