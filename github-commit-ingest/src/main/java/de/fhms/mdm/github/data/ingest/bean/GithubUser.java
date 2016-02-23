package de.fhms.mdm.github.data.ingest.bean;

/**
 * Created by Dave on 18.02.2016.
 * Github User object, kapselt apitoken und login name für Restaufrufe
 */
public class GithubUser {

    private String apiToken;
    private String login;

    public GithubUser(String apiToken, String login){
        this.apiToken  = apiToken;
        this.login = login;
    }

    public String getApiToken() {
        return apiToken;
    }

    public void setApiToken(String apiToken) {
        this.apiToken = apiToken;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    @Override
    public String toString() {
        return "GithubUser{" +
                "apiToken='" + apiToken + '\'' +
                ", login='" + login + '\'' +
                '}';
    }
}
