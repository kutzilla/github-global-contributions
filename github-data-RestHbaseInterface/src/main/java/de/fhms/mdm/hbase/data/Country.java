package de.fhms.mdm.hbase.data;

public class Country {

	
	private String country;

	private double longitude;
	private double latitude;
	private long amount;
	

	public Country(String country, double longitude, double latitude, long amount) {
		super();
		this.country = country;
		this.longitude = longitude;
		this.latitude = latitude;
		this.amount = amount;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public long getAmount() {
		return amount;
	}
	public void setAmount(long amount) {
		this.amount = amount;
	}
	
	public void raiseCommit(){
		this.amount++;
	}
	
	@Override
	public String toString() {
		return "Country [country=" + country + ", amount=" + amount + "]";
	}
	
	
	
	
}
