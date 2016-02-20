package de.fhms.mdm.github_data_processing;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Matthias on 19.02.16.
 */
public class User implements Serializable {

    private static final long serialVersionUID = 10000L;

    private String email;

    private String login;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    @Override
    public String toString() {
        return "User{" +
                "email='" + email + '\'' +
                ", login='" + login + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(email, user.email) &&
                Objects.equals(login, user.login);
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, login);
    }
}
