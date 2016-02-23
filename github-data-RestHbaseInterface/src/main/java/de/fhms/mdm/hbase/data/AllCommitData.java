package de.fhms.mdm.hbase.data;

public class AllCommitData {


	private Integer gesamtAmountCommits;
	private Integer maxAmountCommitsCountry;
	private Integer maxAmountCommitsCity;
	private Country[] countries;
	private City[] cities;


	public AllCommitData(City[] cities, Country[] countries, Integer gesamtAmountCommits, Integer maxAmountCommitsCountry,
			Integer maxAmountCommitsCity) {
		super();
		this.cities = cities;
		this.gesamtAmountCommits = gesamtAmountCommits;
		this.maxAmountCommitsCountry = maxAmountCommitsCountry;
		this.maxAmountCommitsCity = maxAmountCommitsCity;
		this.countries = countries;
	}

	public AllCommitData() {
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

}
