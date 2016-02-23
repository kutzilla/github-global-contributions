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
import de.fhms.mdm.hbase.data.User;

@Path("/json/github")
public class JerseyService {
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"dd.MM.yyyy");
	private final String REPO_ALLE = "ALLE";

	@GET
	@Path("/getAllRepos")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public Repo[] getAllRepos() {
//		/*
		HBaseConnectionManager connMan = new HBaseConnectionManager();
		List<String> repos = connMan.getRepositories();
		Repo[] resultArr = new Repo[repos.size()+1];
		resultArr[0] = new Repo(REPO_ALLE);
		int j = 0;
		for (int i = 1; i < resultArr.length; i++) {
			resultArr[i] = new Repo(repos.get(j));
			j++;
		}
//		*/
		/*
		Repo[] resultArr = new Repo[2];
		resultArr[0] = new Repo(REPO_ALLE);
		resultArr[1] = new Repo("TESTREPO");
		 */
		
		return resultArr;
	}



	@GET
	@Path("/getAllCommitsData")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public AllCommitData getAllCommitsData(@QueryParam("repo") String repo,
			@QueryParam("from") String from, @QueryParam("to") String to) {

		long longVon = getTimeStamp(from);
		long longBis = getTimeStamp(to);
		HBaseConnectionManager connection = new HBaseConnectionManager();
		if(repo !=null && repo.equals(REPO_ALLE)){
			repo = null;
		}
		return connection.getAllCommits(repo, longVon, longBis);
	}
	
	@GET
	@Path("/getAllCommitsDataTest")
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public AllCommitData getAllCommitsDataTest(@QueryParam("repo") String repo,
			@QueryParam("from") String from, @QueryParam("to") String to) {

		long longVon = getTimeStamp(from);
		long longBis = getTimeStamp(to);
		
		User[] users = new User[2];
		User[] users2 = new User[1];
		users2[0] = new User("grawenhoffm","marcel.grawenhoff@gmail.com");
//		User u1 = new User("grawenhoffm","marcel.grawenhoff@gmail.com");
		User u2 = new User("kutzilla","email@gmail.com");
		User u3 = new User("Schleusenfrosch","email@gmail.com");
//		users[0] = u1;
		users[0] = u2; 
		users[1] = u3;
		City[] cities = new City[2];
		City ci1 = new City("Dortmund",  51.5118,7.46495, 1);
		City ci2 = new City("Hamm", 51.96, 7.626, 2);
		cities[0] = ci1;
		cities[1] = ci2;

		Country[] countries = new Country[2];
		Country co1 = new Country("Germany", 0, 0, 300);
		Country co2 = new Country("France", 0, 0, 100);
		countries[0] = co1;
		countries[1] = co2; 
		return new AllCommitData(cities, countries, 400, 300, 2);
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