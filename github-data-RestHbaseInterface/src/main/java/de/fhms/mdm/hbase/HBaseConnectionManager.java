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
import de.fhms.mdm.hbase.data.User;

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

	private static final String COLUMN_FAMILY_USER = "user";
	private static final String COLUMN_USER_EMAIL = "email";
	private static final String COLUMN_USER_LOGIN = "login";

	private static final String COLUMN_FAMILY_COMMITDATA = "commit_data";
	private static final String COLUMN_COMMITDATA_DATE = "date";

	private static final byte[] BYTE_COLUMN_FAMILY_REPODATA = Bytes.toBytes(COLUMN_FAMILY_REPODATA);
	private static final byte[] BYTE_COLUMN_REPODATA_NAME = Bytes.toBytes(COLUMN_REPODATA_NAME);
	private static final byte[] BYTE_COLUMN_REPODATA_OWNER = Bytes.toBytes(COLUMN_REPODATA_OWNER);

	private static final byte[] BYTE_COLUMN_FAMILY_COMMITDATA = Bytes.toBytes(COLUMN_FAMILY_COMMITDATA);
	private static final byte[] BYTE_COLUMN_COMMITDATA_DATE = Bytes.toBytes(COLUMN_COMMITDATA_DATE);

	private static Configuration conf;

	public HBaseConnectionManager() {
		conf = HBaseConfiguration.create();
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
			scan.addColumn(BYTE_COLUMN_FAMILY_REPODATA, BYTE_COLUMN_REPODATA_NAME);
			scan.addColumn(BYTE_COLUMN_FAMILY_REPODATA, BYTE_COLUMN_REPODATA_OWNER);
			rs = table.getScanner(scan);
			while ((res = rs.next()) != null) {

				byte[] repoName = res.getValue(BYTE_COLUMN_FAMILY_REPODATA, BYTE_COLUMN_REPODATA_NAME);
				byte[] repoOwner = res.getValue(BYTE_COLUMN_FAMILY_REPODATA, BYTE_COLUMN_REPODATA_OWNER);

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
		AllCommitData allCommitData = new AllCommitData();
		try {
			HTable table = new HTable(conf, TABLENAME_COMMITS);
			Scan s = new Scan();
			Filter filter = getFilter(repo, from, to);
			if (filter != null) {
				s.setFilter(filter);
			}

			ResultScanner ss = table.getScanner(s);
			
			int i=0;
			for (Result r : ss) {
				handleLocationDatabaseInfos(r, resultCity, resultCountry, allCommitData);
				i++;
			}
			System.out.println("Result Rows Size: "+i);
		} catch (IOException e) {
			e.printStackTrace();
		}

		City[] cities = mapCities(resultCity, allCommitData);
		Country[] countries = mapCountries(resultCountry, allCommitData);
		allCommitData.setCities(cities);
		allCommitData.setCountries(countries);
		return allCommitData;
	}

	private Filter getFilter(String repo, long from, long to) {
		byte[] fromByte = Bytes.toBytes(from + "");
		byte[] toByte = Bytes.toBytes(to + "");
		FilterList allFilters = new FilterList();
		boolean useFilter = false;
		if (from > 0 && to > 0) {
			useFilter = true;
			Filter filterFrom = new SingleColumnValueFilter(BYTE_COLUMN_FAMILY_COMMITDATA, BYTE_COLUMN_COMMITDATA_DATE,
					CompareOp.GREATER_OR_EQUAL, fromByte);
			allFilters.addFilter(filterFrom);

			Filter filterTo = new SingleColumnValueFilter(BYTE_COLUMN_FAMILY_COMMITDATA, BYTE_COLUMN_COMMITDATA_DATE,
					CompareOp.LESS_OR_EQUAL, toByte);
			allFilters.addFilter(filterTo);
		}

		if (repo != null && !repo.isEmpty()) {
			String[] splitRepo = repo.split("/");
			if (splitRepo.length == 2) {
				useFilter = true;
				String owner = splitRepo[0];
				String name = splitRepo[1];
				Filter filterRepoOwner = new SingleColumnValueFilter(BYTE_COLUMN_FAMILY_REPODATA,
						BYTE_COLUMN_REPODATA_OWNER, CompareOp.EQUAL, new RegexStringComparator(owner));
				Filter filterRepoName = new SingleColumnValueFilter(BYTE_COLUMN_FAMILY_REPODATA,
						BYTE_COLUMN_REPODATA_NAME, CompareOp.EQUAL, new RegexStringComparator(name));
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

	private void handleLocationDatabaseInfos(Result r, HashMap<String, City> resultCity,
			HashMap<String, Country> resultCountry, AllCommitData allCommitData) {
		String city = "";
		String country = "";
		String longitude = "";
		String latitude = "";
		String login = "";
		String email = "";

		for (KeyValue kv : r.raw()) {
			String family = new String(kv.getFamily());
			if (family.equals(COLUMN_FAMILY_LOCATION)) {
				String qualifier = new String(kv.getQualifier());
				String value = new String(kv.getValue()).trim();
				if (!value.equals("none")) {
					if (qualifier.equals(COLUMN_LOCATION_CITY)) {
						city = value;
					} else if (qualifier.equals(COLUMN_LOCATION_COUNTRY)) {
						country = value;
					} else if (qualifier.equals(COLUMN_LOCATION_LONGITUDE)) {
						longitude = value;
					} else if (qualifier.equals(COLUMN_LOCATION_LATITUDE)) {
						latitude = value;
					}
				}
			} else if (family.equals(COLUMN_FAMILY_USER)) {
				String qualifier = new String(kv.getQualifier());
				String value = new String(kv.getValue()).trim();
				if (qualifier.equals(COLUMN_USER_LOGIN)) {
					login = value;
				} else if (qualifier.equals(COLUMN_USER_EMAIL)) {
					email = value;
				}
			}
		}
		allCommitData.raiseCommitAmount();
		City cityObj = resultCity.get(city);
		if (cityObj == null) {
			if (!longitude.isEmpty() && !latitude.isEmpty() && !city.isEmpty()) {
				cityObj = new City(city, Double.parseDouble(longitude), Double.parseDouble(latitude), 0);
				resultCity.put(city, cityObj);
			}
		}
		Country countryObj = resultCountry.get(country);
		if (countryObj == null) {
			if (!country.isEmpty()) {
				countryObj = new Country(country, 0, 0, 0);
				resultCountry.put(country, countryObj);
			}
		}

		if (!login.isEmpty()) {
			if (cityObj != null) {
				cityObj.addUser(login, email);
				cityObj.raiseCommit(login);
			}
			if (countryObj != null) {
				countryObj.addUser(login, email);
				countryObj.raiseCommit(login);
			}
		} else {
			if (cityObj != null) {
				cityObj.raiseCommit();
			}
			if (countryObj != null) {
				countryObj.raiseCommit(login);
			}
		}

	}

	private City[] mapCities(HashMap<String, City> resultCity, AllCommitData allCommitData) {
		Iterator<Entry<String, City>> ite1 = resultCity.entrySet().iterator();
		City[] cities = new City[resultCity.size()];
		int maxAmountCommitsCity = 0;
		int i = 0;
		while (ite1.hasNext()) {
			Entry<String, City> set = ite1.next();
			cities[i] = set.getValue();
			if (maxAmountCommitsCity < set.getValue().getAmount()) {
				maxAmountCommitsCity = set.getValue().getAmount();
			}
			i++;
		}
		allCommitData.setMaxAmountCommitsCity(maxAmountCommitsCity);
		return cities;
	}

	private Country[] mapCountries(HashMap<String, Country> resultCountry, AllCommitData allCommitData) {
		Iterator<Entry<String, Country>> ite2 = resultCountry.entrySet().iterator();
		Country[] countries = new Country[resultCountry.size()];
		int maxAmountCommitsCountry = 0;
		int i = 0;
		while (ite2.hasNext()) {
			Entry<String, Country> set = ite2.next();
			countries[i] = set.getValue();
			if (maxAmountCommitsCountry < set.getValue().getAmount()) {
				maxAmountCommitsCountry = set.getValue().getAmount();
			}
			i++;
		}
		allCommitData.setMaxAmountCommitsCountry(maxAmountCommitsCountry);
		return countries;
	}

}
