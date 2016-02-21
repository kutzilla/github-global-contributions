package de.fhms.mdm.github.data.ingest.rest.service;

/**
 * Created by Dave on 18.02.2016.
 * Github User object, kapselt apitoken und login name für Restaufrufe
 */
public class GitHubUser {

    private String apiToken;
    private String login;

    public GitHubUser(String apiToken, String login){
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
        return "GitHubUser{" +
                "apiToken='" + apiToken + '\'' +
                ", login='" + login + '\'' +
                '}';
    }
}
