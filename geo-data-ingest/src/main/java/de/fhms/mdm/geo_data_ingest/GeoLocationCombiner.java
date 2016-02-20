package de.fhms.mdm.geo_data_ingest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Matthias on 18.02.16.
 */
public class GeoLocationCombiner extends Reducer<Text, Text, Text, Text> {
}
