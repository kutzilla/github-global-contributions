package de.fhms.mdm.hbase.data;

public class AllCommitData {


	private int gesamtAmountCommits;
	private int maxAmountCommitsCountry;
	private int maxAmountCommitsCity;
	private Country[] countries;
	private City[] cities;

	public AllCommitData(){
		gesamtAmountCommits = 0;
		maxAmountCommitsCountry = 0;
		maxAmountCommitsCity = 0;
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

	public Integer getGesamtAmountCommits() {
		return gesamtAmountCommits;
	}

	public void setGesamtAmountCommits(Integer gesamtAmountCommits) {
		this.gesamtAmountCommits = gesamtAmountCommits;
	}

	public Integer getMaxAmountCommitsCountry() {
		return maxAmountCommitsCountry;
	}

	public void setMaxAmountCommitsCountry(Integer maxAmountCommitsCountry) {
		this.maxAmountCommitsCountry = maxAmountCommitsCountry;
	}

	public Integer getMaxAmountCommitsCity() {
		return maxAmountCommitsCity;
	}

	public void setMaxAmountCommitsCity(Integer maxAmountCommitsCity) {
		this.maxAmountCommitsCity = maxAmountCommitsCity;
	}


	public void raiseCommitAmount() {
		this.gesamtAmountCommits++;
		
	}

}
