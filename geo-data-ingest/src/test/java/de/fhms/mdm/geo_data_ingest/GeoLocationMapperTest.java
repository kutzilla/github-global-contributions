package de.fhms.mdm.geo_data_ingest;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationMapperTest {

    @Test
    public void testLocationMapping() throws Exception {
        String testValue1 = "40c4ffbb46f74a52a230f983b8c4ba4a7ca8e1fe;" +
                "remove demo link from README;Tue Jan 12 08:09:38 PST 2016;kutzilla;" +
                "kutz.matthias@gmail.com;Münster, Germany;Tue Jan 12 08:09:50 PST 2016";
        String testValue2 = "40c4ffbb46f74a52a230f983b8c4ba4a7ca8e1fe;" +
                "second commitd" +
                ";Tue Jan 12 12:45:59 PST 2016;kutzilla;" +
                "kutz.matthias@gmail.com;Münster, Deutschland;Tue Jan 12 12:45:55 PST 2016";
        new MapDriver<LongWritable, Text, Text, IntWritable>().withMapper(new GeoLocationMapper())
                .withInput(new LongWritable(), new Text(testValue1))
                .withInput(new LongWritable(), new Text(testValue2))
                .withOutput(new Text("Münster, Germany"), new IntWritable(1))
                .withOutput(new Text("Münster, Deutschland"), new IntWritable(1))
                .runTest();
    }
}
