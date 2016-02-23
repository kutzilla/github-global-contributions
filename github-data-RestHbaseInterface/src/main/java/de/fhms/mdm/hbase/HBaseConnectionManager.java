package de.fhms.mdm.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import de.fhms.mdm.hbase.data.City;
import de.fhms.mdm.hbase.data.AllCommitData;
import de.fhms.mdm.hbase.data.Country;

public class HBaseConnectionManager {
	private static final String TABLENAME_COMMITS = "Commits";

	private static final String COLUMN_FAMILY_REPODATA = "repo_data";
	private static final String COLUMN_REPODATA_NAME = "name";
	private static final String COLUMN_REPODATA_OWNER = "owner";

	private static final String COLUMN_FAMILY_LOCATION = "location";
	private static final String COLUMN_LOCATION_CITY = "city";
	private static final String COLUMN_LOCATION_COUNTRY = "country";
	private static final String COLUMN_LOCATION_LONGITUDE = "longitude";
	private static final String COLUMN_LOCATION_LATITUDE = "latitude";

	private static final String COLUMN_FAMILY_COMMITDATA = "commit_data";
	private static final String COLUMN_COMMITDATA_DATE = "date";

	private static final byte[] BYTE_COLUMN_FAMILY_REPODATA = Bytes
			.toBytes(COLUMN_FAMILY_REPODATA);
	private static final byte[] BYTE_COLUMN_REPODATA_NAME = Bytes
			.toBytes(COLUMN_REPODATA_NAME);
	private static final byte[] BYTE_COLUMN_REPODATA_OWNER = Bytes
			.toBytes(COLUMN_REPODATA_OWNER);

	private static final byte[] BYTE_COLUMN_FAMILY_COMMITDATA = Bytes
			.toBytes(COLUMN_FAMILY_COMMITDATA);
	private static final byte[] BYTE_COLUMN_COMMITDATA_DATE = Bytes
			.toBytes(COLUMN_COMMITDATA_DATE);

	private static Configuration conf;

	public HBaseConnectionManager() {
		conf = HBaseConfiguration.create();
	}

	/**
	 * Scan (or list) a table
	 */
	public static void getAllCommits() {
		try {
			HTable table = new HTable(conf, TABLENAME_COMMITS);
			byte[] WERT = Bytes.toBytes("1444679400000");
			Filter filter = new SingleColumnValueFilter(
					BYTE_COLUMN_FAMILY_COMMITDATA, BYTE_COLUMN_COMMITDATA_DATE,
					CompareOp.GREATER_OR_EQUAL, WERT);
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				System.out.println("############NEW ROW");
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> getRepositories() {

		List<String> resultList = new ArrayList<String>();
		ResultScanner rs = null;
		Result res = null;
		String sOwner = null;
		String sName = null;
		String s = null;
		try {
			HTable table = new HTable(conf, TABLENAME_COMMITS);
			Scan scan = new Scan();
			scan.addColumn(BYTE_COLUMN_FAMILY_REPODATA,
					BYTE_COLUMN_REPODATA_NAME);
			scan.addColumn(BYTE_COLUMN_FAMILY_REPODATA,
					BYTE_COLUMN_REPODATA_OWNER);
			rs = table.getScanner(scan);
			while ((res = rs.next()) != null) {

				byte[] repoName = res.getValue(BYTE_COLUMN_FAMILY_REPODATA,
						BYTE_COLUMN_REPODATA_NAME);
				byte[] repoOwner = res.getValue(BYTE_COLUMN_FAMILY_REPODATA,
						BYTE_COLUMN_REPODATA_OWNER);

				sOwner = Bytes.toString(repoOwner);
				sName = Bytes.toString(repoName);
				s = sOwner + "/" + sName;
				if (!resultList.contains(s)) {
					resultList.add(s);
				}
			}
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		} finally {
			rs.close();
		}
		return resultList;
	}

	public AllCommitData getAllCommits(String repo, long from, long to) {
		HashMap<String, City> resultCity = new HashMap<String, City>();
		HashMap<String, Country> resultCountry = new HashMap<String, Country>();
		try {
			HTable table = new HTable(conf, TABLENAME_COMMITS);

			Scan s = new Scan();
			Filter filter = getFilter(repo, from, to);
			if (filter != null) {
				s.setFilter(filter);
			}

			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				handleLocationDatabaseInfos(r, resultCity, resultCountry);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		City[] cities = mapCities(resultCity);
		Country[] countries = mapCountries(resultCountry);

		return new AllCommitData(cities, countries);
	}

	private Filter getFilter(String repo, long from, long to) {
		byte[] fromByte = Bytes.toBytes(from + "");
		byte[] toByte = Bytes.toBytes(to + "");
		FilterList allFilters = new FilterList();
		boolean useFilter = false;
		if (from > 0 && to > 0) {
			useFilter = true;
			Filter filterFrom = new SingleColumnValueFilter(
					BYTE_COLUMN_FAMILY_COMMITDATA, BYTE_COLUMN_COMMITDATA_DATE,
					CompareOp.GREATER_OR_EQUAL, fromByte);
			allFilters.addFilter(filterFrom);

			Filter filterTo = new SingleColumnValueFilter(
					BYTE_COLUMN_FAMILY_COMMITDATA, BYTE_COLUMN_COMMITDATA_DATE,
					CompareOp.LESS_OR_EQUAL, toByte);
			allFilters.addFilter(filterTo);
		}

		if (repo != null && !repo.isEmpty()) {
			useFilter = true;
			String[] splitRepo = repo.split("/");
			if (splitRepo.length == 2) {

				String owner = splitRepo[0];
				String name = splitRepo[1];
				Filter filterRepoOwner = new SingleColumnValueFilter(
						BYTE_COLUMN_FAMILY_REPODATA,
						BYTE_COLUMN_REPODATA_OWNER, CompareOp.EQUAL,
						new RegexStringComparator(owner));
				Filter filterRepoName = new SingleColumnValueFilter(
						BYTE_COLUMN_FAMILY_COMMITDATA,
						BYTE_COLUMN_REPODATA_NAME, CompareOp.EQUAL,
						new RegexStringComparator(name));

				allFilters.addFilter(filterRepoOwner);
				allFilters.addFilter(filterRepoName);
			}
		}
		if (useFilter) {
			return allFilters;
		} else {
			return null;
		}
	}

	private void handleLocationDatabaseInfos(Result r,
			HashMap<String, City> resultCity,
			HashMap<String, Country> resultCountry) {
		String city = "";
		String country = "";
		String longitude = "0";
		String latitude = "0";
		for (KeyValue kv : r.raw()) {
			String family = new String(kv.getFamily());
			if (family.equals(COLUMN_FAMILY_LOCATION)) {
				String qualifier = new String(kv.getQualifier());
				if (qualifier.equals(COLUMN_LOCATION_CITY)) {
					city = new String(kv.getValue());
				} else if (qualifier.equals(COLUMN_LOCATION_COUNTRY)) {
					country = new String(kv.getValue());
				} else if (qualifier.equals(COLUMN_LOCATION_LONGITUDE)) {
					longitude = new String(kv.getValue());
				} else if (qualifier.equals(COLUMN_LOCATION_LATITUDE)) {
					latitude = new String(kv.getValue());
				}
			}
		}
		City cityObj = resultCity.get(city);
		if (cityObj == null) {
			cityObj = new City(city, Double.parseDouble(longitude),
					Double.parseDouble(latitude), 0);
			resultCity.put(city, cityObj);
		}
		cityObj.raiseCommit();
		Country countryObj = resultCountry.get(country);
		if (countryObj == null) {
			countryObj = new Country(country, 0, 0, 0);
			resultCountry.put(country, countryObj);
		}
		countryObj.raiseCommit();
	}

	private City[] mapCities(HashMap<String, City> resultCity) {
		Iterator<Entry<String, City>> ite1 = resultCity.entrySet().iterator();
		City[] cities = new City[resultCity.size()];
		int i = 0;
		while (ite1.hasNext()) {
			Entry<String, City> set = ite1.next();
			cities[i] = set.getValue();
			i++;
		}
		return cities;
	}

	private Country[] mapCountries(HashMap<String, Country> resultCountry) {
		Iterator<Entry<String, Country>> ite2 = resultCountry.entrySet()
				.iterator();
		Country[] countries = new Country[resultCountry.size()];
		int i = 0;
		while (ite2.hasNext()) {
			Entry<String, Country> set = ite2.next();
			countries[i] = set.getValue();
			i++;
		}
		return countries;
	}

}
