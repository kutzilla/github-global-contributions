package de.fhms.mdm.github_user_ingest.service;

import de.fhms.mdm.github_user_ingest.util.ServiceUtil;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;


/**
 * Created by Dave on 21.02.2016.
 */
public class GitHubUserService {
    private static final String URI = "https://api.github.com/users";
    private GithubUser user;
    private RestTemplate restTemplate;

    public GitHubUserService(GithubUser user){
        if(user != null){
            this.user = user;
        }
        this.restTemplate = new RestTemplate();
    }

    //gibt einzelne UserDaten zurück
    public ResponseEntity<Object> getUser(String userLogin){
        System.out.println("Entering getUser[" + userLogin +"]");
        String uri = URI + "/" + userLogin;
        boolean userExist = true;

        System.out.println("Requesting with Uri: " + uri);

        HttpHeaders headers = ServiceUtil.createHeadersWithCredentials(this.user);
        HttpEntity<String> request = new HttpEntity<String>("", headers);

        if(doesUserExist(userLogin)) {
            ResponseEntity<Object> responseEntity = restTemplate.exchange(uri, HttpMethod.GET, request, Object.class);
            return responseEntity;
        }
        return null;
    }

    private boolean doesUserExist(String userLogin){
        String uri = URI + "/" + userLogin;
        HttpHeaders headers = ServiceUtil.createHeadersWithCredentials(this.user);
        HttpEntity<String> request = new HttpEntity<String>("", headers);
        boolean userExist = true;

        ResponseEntity<Object> responseEntity = null;
        try {
            responseEntity = restTemplate.exchange(uri, HttpMethod.HEAD, request, Object.class);
        }catch (HttpClientErrorException e){
            System.out.println("User not found");
            userExist = false;
        }
        return userExist;
    }

}
