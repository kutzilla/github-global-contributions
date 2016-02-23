package de.fhms.mdm.geo_data_ingest;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Matthias on 22.02.16.
 */
public class GeoLocationTableMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static final String COLUMN_LONGITUDE = "longitude";

    private static final String COLUMN_LATITUDE = "latitude";

    public static final String GOOGLE_GEO_API_KEY = "AIzaSyBs03q5sQG6SCz8ytna3VWmL-gX5Y4mGEU";

    private static final String NONE_VALUE = "none";

    private GeoApiContext geoApiContext;

    public GeoLocationTableMapper() {
        geoApiContext = new GeoApiContext().setApiKey(GOOGLE_GEO_API_KEY);
    }


    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        System.setProperty("http.proxyHost",GeoLocationFetcher.PROXY);
        System.setProperty("http.proxyPort","8080");
        System.setProperty("https.proxyHost",GeoLocationFetcher.PROXY);
        System.setProperty("https.proxyPort","8080");

        String key = new String(row.get());

        byte[] longitudeBytes = value.getValue(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                Bytes.toBytes(COLUMN_LONGITUDE));
        byte[] latitudeBytes = value.getValue(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                Bytes.toBytes(COLUMN_LATITUDE));

        String longitude = new String(longitudeBytes);
        String latitude = new String(latitudeBytes);

        if (longitude.equals(NONE_VALUE) && latitude.equals(NONE_VALUE)) {
            String geoLocation = locate(key);
            if (geoLocation != null) {
                String lon = geoLocation.split(",")[0];
                String lat = geoLocation.split(",")[1];
                Result result = new Result();
                Put longPut = new Put(row.get());
                longPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_LONGITUDE), Bytes.toBytes(lon));
                Put latPut = new Put(row.get());
                latPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_LATITUDE), Bytes.toBytes(lat));
                context.write(row, longPut);
                context.write(row, latPut);
            }
        }
    }

    private String locate(String address) {
        try {
            GeocodingResult[] results  = GeocodingApi.geocode(geoApiContext, address).await();
            if (results.length > 0) {
                return results[0].geometry.location.toString();
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
