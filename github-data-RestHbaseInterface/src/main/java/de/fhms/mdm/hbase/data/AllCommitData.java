package de.fhms.mdm.hbase.data;

public class AllCommitData {

	
	private City[] cities;
	private Country[] countries;
	
	public AllCommitData(City[] cities, Country[] countries) {
		super();
		this.cities = cities;
		this.countries = countries;
	}
	public City[] getCities() {
		return cities;
	}
	public void setCities(City[] cities) {
		this.cities = cities;
	}
	public Country[] getCountries() {
		return countries;
	}
	public void setCountries(Country[] countries) {
		this.countries = countries;
	}
	
	
	
}
