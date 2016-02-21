package de.fhms.mdm.github_user_ingest;

import de.fhms.mdm.github.data.ingest.bean.GithubRepoId;
import de.fhms.mdm.github.data.ingest.bean.GithubUser;
import org.springframework.http.ResponseEntity;

/**
 * Created by Dave on 21.02.2016.
 */
public class UserServiceTest {
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";

    public static void main(String[] args){
        GithubRepoId repoId = new GithubRepoId("kutzilla","marlin-cm");
        GithubUser user = new GithubUser(API_TOKEN,"schleusenfrosch");
        GitHubUserService userService = new GitHubUserService(user);

        String userLogin = "kutzilla";

        ResponseEntity<Object> response =  userService.getUser(userLogin);

        if(response != null) {
            System.out.println(response.getBody().toString());
        }

    }
}
