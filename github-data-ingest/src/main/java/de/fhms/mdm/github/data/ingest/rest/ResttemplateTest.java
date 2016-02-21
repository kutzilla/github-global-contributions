package de.fhms.mdm.github.data.ingest.rest;


import de.fhms.mdm.github.data.ingest.rest.service.GithubRepoId;
import de.fhms.mdm.github.data.ingest.rest.service.GithubUser;
import de.fhms.mdm.github.data.ingest.rest.service.GithubCommitService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.ResponseEntity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Dave on 18.02.2016.
 */


public class ResttemplateTest {
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";
    private static final String LAST_ETAG = "W/\"ba6ba125a032855ad4bc4c08b85f3cf54\"";
    public static void main(String[] args){


        GithubRepoId repoId = new GithubRepoId("kutzilla","marlin-cm");
        GithubUser user = new GithubUser(API_TOKEN,"schleusenfrosch");
        GithubCommitService eventService = new GithubCommitService(user,repoId,LAST_ETAG);

        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz",Locale.ENGLISH);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = null;

        try {
            date = format.parse("Thu, 01 Jan 2014 10:13:01 GMT");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(date);
        if(eventService.areNewCommitsAvailableDate(date)) {
            List<ResponseEntity<Object[]>> pagedCommits = eventService.getCommitsSince(date);

            for (ResponseEntity<Object[]> page : pagedCommits) {
                JSONArray array = new JSONArray(page.getBody());
                for (int i = 0; i < array.length(); i++) {
                    System.out.println(array.get(i).toString());
                    JSONObject wholeCommit = (JSONObject) array.get(i);
                    JSONObject committer = (JSONObject) wholeCommit.get("committer");
                    if(committer != null) {
                        String login = committer.getString("login");
                        System.out.println(login);
                    }
                }
            }
        }



        /*
        ResponseEntity<Object[]> responseEntity = eventService.getCommitsSince(date);
        System.out.println(Arrays.toString(responseEntity.getBody()));
        */


        //eventService.test();


    }
}
