package de.fhms.mdm.hbase.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class City {

	private String city;
	private double longitude;
	private double latitude;
	private Integer amount;
	private List<User> users;

	public City(String city, double longitude, double latitude, int amount) {
		super();
		this.city = city;
		this.longitude = longitude;
		this.latitude = latitude;
		this.amount = amount;
		this.users = new ArrayList<User>();
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

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public void raiseCommit() {
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
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
		City other = (City) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		return true;
	}


}
