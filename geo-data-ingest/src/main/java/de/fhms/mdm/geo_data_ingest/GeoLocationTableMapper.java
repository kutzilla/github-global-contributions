package de.fhms.mdm.geo_data_ingest;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.AddressComponent;
import com.google.maps.model.AddressComponentType;
import com.google.maps.model.GeocodingResult;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by Matthias on 22.02.16.
 */
public class GeoLocationTableMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static final String COLUMN_LONGITUDE = "longitude";

    private static final String COLUMN_LATITUDE = "latitude";

    private static final String COLUMN_CITY = "city";

    private static final String COLUMN_COUNTRY = "country";

    public static final String GOOGLE_GEO_API_KEY = "AIzaSyBs03q5sQG6SCz8ytna3VWmL-gX5Y4mGEU";

    private static final String NONE_VALUE = "none";

    private GeoApiContext geoApiContext;

    public GeoLocationTableMapper() {
        geoApiContext = new GeoApiContext().setApiKey(GOOGLE_GEO_API_KEY);
    }


    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        //Proxy setzen, wenn auf Cluster VM ausgeführt
        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration<InetAddress> ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                if (i.getHostAddress().toString().equals("10.60.67.4")) {
                    System.out.println("########################### Setting Proxy ###########################");
                    System.setProperty("http.proxyHost", "10.60.17.102");
                    System.setProperty("http.proxyPort", "8080");
                    System.setProperty("https.proxyHost", "10.60.17.102");
                    System.setProperty("https.proxyPort", "8080");
                }
            }
        }
        String key = new String(row.get());

        byte[] longitudeBytes = value.getValue(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                Bytes.toBytes(COLUMN_LONGITUDE));
        byte[] latitudeBytes = value.getValue(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                Bytes.toBytes(COLUMN_LATITUDE));

        String longitude = new String(longitudeBytes);
        String latitude = new String(latitudeBytes);

        if (longitude.equals(NONE_VALUE) && latitude.equals(NONE_VALUE)) {
            String geoLocation[] = locate(key);
            if (geoLocation != null) {
                String city = geoLocation[0];
                String country = geoLocation[1];
                String lon = geoLocation[2];
                String lat = geoLocation[3];
                Put longPut = new Put(row.get());
                longPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_LONGITUDE), Bytes.toBytes(lon));
                Put latPut = new Put(row.get());
                latPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_LATITUDE), Bytes.toBytes(lat));
                Put cityPut = new Put(row.get());
                cityPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_CITY), Bytes.toBytes(city));
                Put countryPut = new Put(row.get());
                countryPut.add(Bytes.toBytes(GeoLocationFetcher.LOCATION_COLUMN_FAMILY),
                        Bytes.toBytes(COLUMN_COUNTRY), Bytes.toBytes(country));
                context.write(row, longPut);
                context.write(row, latPut);
                context.write(row, cityPut);
                context.write(row, countryPut);
            }
        }
    }

    public String[] locate(String address) {
        GeocodingResult[] results = null;
        try {
            results  = GeocodingApi.geocode(geoApiContext, address).await();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (results != null && results.length > 0) {
            GeocodingResult firstResult = results[0];
            AddressComponent[] addressComponent = firstResult.addressComponents;

            int localityIndex = -1;
            int countryIndex = -1;
            for (int i = 0; i < addressComponent.length; i++) {
                List<AddressComponentType> v = Arrays.asList(addressComponent[i].types);
                if (v.contains(AddressComponentType.LOCALITY)) {
                    localityIndex = i;
                }
                if (v.contains(AddressComponentType.COUNTRY)) {
                    countryIndex = i;
                }
            }
            if (localityIndex != -1 && countryIndex != -1) {
                String city = addressComponent[localityIndex].longName;
                String country = addressComponent[countryIndex].longName;
                String[] longlat = firstResult.geometry.location.toString().split(",");
                String longitude = longlat[0];
                String latitude = longlat[1];
                System.out.println(country);
                return new String[] {city, country, longitude, latitude};
            } else if (localityIndex == -1 && countryIndex >= 0) {
                String city = "none";
                String country = addressComponent[countryIndex].longName;
                String[] longlat = firstResult.geometry.location.toString().split(",");
                String longitude = longlat[0];
                String latitude = longlat[1];
                System.out.println(country);
                return new String[] {city, country, longitude, latitude};
            }
        }
        return null;
    }

}
