package de.fhms.mdm.github_user_ingest.service;


import org.json.JSONObject;
import org.springframework.http.ResponseEntity;

/**
 * Created by Dave on 21.02.2016.
 */
public class UserServiceTest {
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";

    public static void main(String[] args){
        GithubRepoId repoId = new GithubRepoId("kutzilla","marlin-cm");
        GithubUser user = new GithubUser(API_TOKEN,"schleusenfrosch");
        GitHubUserService gitHubUserService = new GitHubUserService(user);

        String userLogin = "marckleinebudde";

        ResponseEntity<String> responseEntity = gitHubUserService.getUser(userLogin);
        System.out.println("Body: " + responseEntity.getBody());
        JSONObject userObject = new JSONObject(responseEntity.getBody());

        System.out.println(userObject.toString());

        if (userObject.has("location")) {
            if (!userObject.isNull("location")) {
                String location = userObject.getString("location");
                System.out.println(location);
            }

        }


    }
}
