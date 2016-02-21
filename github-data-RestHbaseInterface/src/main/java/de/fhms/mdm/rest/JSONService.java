package de.fhms.mdm.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import de.fhms.mdm.hbase.data.City;
import de.fhms.mdm.hbase.data.Country;
import de.fhms.mdm.hbase.data.Repo;

@Path("/json/github")
public class JSONService {
	
	@GET
	@Path("/getAllRepos")
	@Produces(MediaType.APPLICATION_JSON+ ";charset=utf-8")
	public Repo[] getAllRepos() {
		Repo[] repos = new Repo[3];
		repos[0] = new Repo("Alle");
		repos[1] = new Repo("Linux-Kernal");
		repos[2] = new Repo("bde-project");
		return repos;
	}
	
	@GET
	@Path("/getAllCommitsOfAllCountries")
	@Produces(MediaType.APPLICATION_JSON+ ";charset=utf-8")
	public Country[] getAllCommitsOfAllCountries(@QueryParam("repo") String repo, @QueryParam("from") String from,
			@QueryParam("to") String to) {
		Country[] countries = new Country[4];
		countries[0] = new Country("Germany", 51.0782773, 5.9696593, 900);
		countries[1] = new Country("United States", 36.216232, -113.6860812, 600);
		countries[2] = new Country("France", 46.1279128, -2.2774104, 350);
		countries[3] = new Country("Russian Federation", 49.7813466,68.8975911, 550);
		return countries;
	}
	
	@GET 
	@Path("/getAllCommitsOfAllCities")
	@Produces(MediaType.APPLICATION_JSON+ ";charset=utf-8")
	public City[] getAllCities(@QueryParam("repo") String repo, @QueryParam("from") String from,
			@QueryParam("to") String to) {
		City[] cities = new City[3];
		cities[0] = new City("Münster",51.9500023,7.484016, 50);
		cities[1] = new City("Hamm",51.6611642,7.695978,10);
		cities[2] = new City("Köln",50.9571612,6.8272413,13);
		return cities; 
	}
	
}