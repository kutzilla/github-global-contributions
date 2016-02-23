package de.fhms.mdm.rest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import de.fhms.mdm.hbase.HBaseConnectionManager;
import de.fhms.mdm.hbase.data.AllCommitData;
import de.fhms.mdm.hbase.data.City;
import de.fhms.mdm.hbase.data.Country;
import de.fhms.mdm.hbase.data.Repo;

@Path("/json/github")
public class JerseyService {
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"dd.MM.yyyy");

	@GET
	@Path("/getAllRepos")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public Repo[] getAllRepos() {
		
		HBaseConnectionManager connMan = new HBaseConnectionManager();
		List<String> repos = connMan.getRepositories();
		Repo[] resultArr = new Repo[repos.size()];
		for (int i = 0; i < repos.size(); i++) {
			resultArr[i] = new Repo(repos.get(i));
		}
		/**
		Repo[] resultArr = new Repo[1];
		resultArr[0] = new Repo("TESTREPO");
		*/
		return resultArr;
	}


	@GET
	@Path("/getAllCommitsOfAllCountries")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public Country[] getAllCommitsOfAllCountries(
			@QueryParam("repo") String repo, @QueryParam("from") String from,
			@QueryParam("to") String to) {

		long longVon = getTimeStamp(from);
		long longBis = getTimeStamp(to);

		Country[] countries = new Country[4];
		countries[0] = new Country("Germany", 51.0782773, 5.9696593, 900);
		countries[1] = new Country("United States", 36.216232, -113.6860812,
				100);
		countries[2] = new Country("France", 46.1279128, -2.2774104, 350);
		countries[3] = new Country("Russian Federation", 49.7813466,
				68.8975911, 550);
		return countries;
	}

	@GET
	@Path("/getAllCommitsOfAllCities")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public City[] getAllCities(@QueryParam("repo") String repo,
			@QueryParam("from") String from, @QueryParam("to") String to) {
		City[] cities = new City[3];
		cities[0] = new City("Münster", 51.9500023, 7.484016, 50);
		cities[1] = new City("Hamm", 51.6611642, 7.695978, 10);
		cities[2] = new City("Köln", 50.9571612, 6.8272413, 13);
		return cities;
	}

	@GET
	@Path("/getAllCommitsData")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public AllCommitData getAllCommitsData(@QueryParam("repo") String repo,
			@QueryParam("from") String from, @QueryParam("to") String to) {

		long longVon = getTimeStamp(from);
		long longBis = getTimeStamp(to);
		HBaseConnectionManager connection = new HBaseConnectionManager();

		return connection.getAllCommits(repo, longVon, longBis);
	}
	
	@GET
	@Path("/getAllCommitsDataTest")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public AllCommitData getAllCommitsDataTest(@QueryParam("repo") String repo,
			@QueryParam("from") String from, @QueryParam("to") String to) {

		long longVon = getTimeStamp(from);
		long longBis = getTimeStamp(to);
		City[] cities = new City[2];
		City ci1 = new City("Dortmund",  51.5118,7.46495, 100);
		City ci2 = new City("Hamm", 51.96, 7.626, 200);
		cities[0] = ci1;
		cities[1] = ci2;
		
		Country[] countries = new Country[2];
		Country co1 = new Country("Germany", 0, 0, 300);
		Country co2 = new Country("France", 0, 0, 100);
		countries[0] = co1;
		countries[1] = co2;
		return new AllCommitData(cities, countries);
	}

	private long getTimeStamp(String time) {
		Date date;
		if (time != null) {
			try {
				date = dateFormat.parse(time);
				return date.getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

}