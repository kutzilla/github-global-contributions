package de.fhms.mdm.geo_data_ingest;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationReducer extends Reducer<Text, IntWritable, Text, Text> {

    public static final String GOOGLE_GEO_API_KEY = "AIzaSyBs03q5sQG6SCz8ytna3VWmL-gX5Y4mGEU";


    private GeoApiContext geoApiContext;

    public GeoLocationReducer() {
        geoApiContext = new GeoApiContext().setApiKey(GOOGLE_GEO_API_KEY);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        try {
            String location = locate(key.toString());
            context.write(key, new Text(location));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String locate(String address) throws Exception {
        GeocodingResult[] results  = GeocodingApi.geocode(geoApiContext, address).await();
        if (results.length > 0) {
            return results[0].geometry.location.toString();
        } else {
            return null;
        }
    }

    public GeoApiContext getGeoApiContext() {
        return geoApiContext;
    }

    public void setGeoApiContext(GeoApiContext geoApiContext) {
        this.geoApiContext = geoApiContext;
    }
}
