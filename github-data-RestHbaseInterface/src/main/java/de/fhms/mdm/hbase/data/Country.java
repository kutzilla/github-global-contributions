package de.fhms.mdm.hbase.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Country {

	
	private String country;

	private double longitude;
	private double latitude;
	private int amount;
	private List<User> users;

	public Country(String country, double longitude, double latitude, int amount) {
		super();
		this.country = country;
		this.longitude = longitude;
		this.latitude = latitude;
		this.amount = amount;
		this.users = new ArrayList<User>();
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
	public int getAmount() {
		return amount;
	}
	public void setAmount(int amount) {
		this.amount = amount;
	}
	
	public void raiseCommit(){
		this.amount++;
	}
	
	public List<User> getUsers() {
		return users;
	}
	public void setUsers(List<User> users) {
		this.users = users;
	}

	public void addUser(User newUsers) {

		if (!this.users.contains(newUsers)) {
			this.users.add(newUsers);
		}

	}
	@Override
	public String toString() {
		return "Country [country=" + country + ", longitude=" + longitude + ", latitude=" + latitude + ", amount="
				+ amount + ", users=" + users + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Country other = (Country) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		return true;
	}
	
	
	
	
	
	
	
	
	
}
