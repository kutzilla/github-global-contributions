package de.fhms.mdm.hbase.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Country {

	private String country;

	private double longitude;
	private double latitude;
	private int amount;
	private ArrayList<User> users;

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

	public void raiseCommit(String login) {
		for (int i = 0; i < this.users.size(); i++) {
			if (login.equals(this.users.get(i).getLogin())) {
				this.users.get(i).raiseCommits();
				break;
			}
		}
		this.amount++;
	}

	public void raiseCommit() {
		this.amount++;
	}

	public ArrayList<User> getUsers() {
		Collections.sort(this.users, new Comparator<User>() {
			@Override
			public int compare(User user2, User user1) {
				return user1.getAmount().compareTo(user2.getAmount());
			}
		});
		return users;
	}

	public void setUsers(ArrayList<User> users) {
		this.users = users;
	}

	public void addUser(String login, String email) {
		boolean found = false;
		for (int i = 0; i < users.size(); i++) {
			if(users.get(i).getLogin().equals(login)){
				found = true;
			}
		}
		if(!found){
			this.users.add(new User(login, email));
		}
	}

	public static Comparator<User> UserComparator = new Comparator<User>() {

		public int compare(User u1, User u2) {

			Integer fruitName1 = u1.getAmount();
			Integer fruitName2 = u2.getAmount();

			// ascending order
			// return fruitName1.compareTo(fruitName2);

			// descending order
			return fruitName2.compareTo(fruitName1);
		}

	};

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
