package de.fhms.mdm.geo_data_ingest;

import com.google.maps.GeoApiContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationReducerTest {

    private GeoApiContext geoApiContext;

    @Before
    public void setUp() {
        geoApiContext = new GeoApiContext().setApiKey(GeoLocationReducer.GOOGLE_GEO_API_KEY);
    }

    @Test
    public void testLocationMapping() throws Exception {
        GeoLocationReducer geoLocationReducer = new GeoLocationReducer();
        geoLocationReducer.setGeoApiContext(geoApiContext);

        new ReduceDriver<Text, IntWritable, Text, Text>().withReducer(geoLocationReducer)
                .withInput(new Text("Münster"), Arrays.asList(new IntWritable(1)))
                .withOutput(new Text("Münster"), new Text("51.960665,7.626135"))
                .runTest();
    }
}
