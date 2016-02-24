package de.fhms.mdm.hbase.data;

public class User implements Comparable<User> {
	private String login;
	private String email;
	private Integer amount;

	public User(String login, String email) {
		super();
		this.login = login;
		this.email = email;
		amount = 0;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	

	public Integer getAmount() {
		return amount;
	}

	public void setAmount(Integer amount) {
		this.amount = amount;
	}

	public void raiseCommits(){
		this.amount++;
	}
	


	@Override
	public String toString() {
		return "User [login=" + login + ", email=" + email + ", amount=" + amount + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((login == null) ? 0 : login.hashCode());
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
		User other = (User) obj;
		if (login == null) {
			if (other.login != null)
				return false;
		} else if (!login.equals(other.login))
			return false;
		return true;
	}

	@Override
    public int compareTo(User o) {
        return this.amount.compareTo(o.getAmount());
    }

	
	
}
