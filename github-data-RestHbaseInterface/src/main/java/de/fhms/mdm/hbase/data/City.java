package de.fhms.mdm.hbase.data;

public class City {
 
	
	private String city;
	private double longitude;
	private double latitude;
	private long amount;
	
	public City(String city, double longitude, double latitude, long amount) {
		super();
		this.city = city;
		this.longitude = longitude;
		this.latitude = latitude;
		this.amount = amount;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
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
		return "City [city=" + city + ", longitude=" + longitude
				+ ", latitude=" + latitude + ", amount=" + amount + "]";
	}
	
	
	
}
