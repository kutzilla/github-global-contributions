package de.fhms.mdm.github.data.ingest.rest.service;

import de.fhms.mdm.github.data.ingest.util.ServiceUtil;
import org.apache.commons.net.util.Base64;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by Dave on 18.02.2016.
 */
public class GithubCommitService {
    private static final String URI = "https://api.github.com/repos";

    private GithubUser user;
    private GithubRepoId repoId;
    private RestTemplate restTemplate;
    private String lastEtag;

    public GithubCommitService(GithubUser user, GithubRepoId repoId, String lastEtag){
        if(user != null){
            this.user = user;
        }
        this.repoId = repoId;
        this.restTemplate = new RestTemplate();
        this.lastEtag = lastEtag == null ? "":lastEtag;
    }

    public boolean areNewCommitsAvailableEtag(String eTag){
        System.out.println("areNewCommitsAvailable [lastEtag:" + lastEtag+ "]");
        String uri = createUri();
        uri += "/commits";
        HttpHeaders headers = ServiceUtil.createHeadersWithCredentials(this.user);
        headers.add(HttpHeaders.IF_NONE_MATCH, lastEtag);
        HttpEntity<String> request = new HttpEntity<String>("",headers);

        ResponseEntity<Object> responseEntity = restTemplate.exchange(uri,HttpMethod.HEAD,request,Object.class);

        int status = responseEntity.getStatusCode().value();
        System.out.println("areNewCommitsAvailable returned Status Code:" + status);
        return  status == HttpStatus.OK.value();
    }

    public boolean areNewCommitsAvailableDate(Date since){
        String uri = createUri();
        uri += "/commits";

        HttpHeaders headers = ServiceUtil.createHeadersWithCredentials(this.user);
        headers.add(HttpHeaders.IF_MODIFIED_SINCE, ServiceUtil.getIsoDateString(since));
        HttpEntity<String> request = new HttpEntity<String>("",headers);

        ResponseEntity<Object> responseEntity = restTemplate.exchange(uri,HttpMethod.HEAD,request,Object.class);

        int status = responseEntity.getStatusCode().value();
        System.out.println("areNewCommitsAvailable returned Status Code:" + status);
        return  status == HttpStatus.OK.value();
    }

    //gibt Liste mit jeweils 30 Commits zurück (aufgrund der Paginierung von Github)
    public List<ResponseEntity<Object[]>> getCommitsSince(Date since){
        System.out.println("Entering getCommitsSince[" + since.toString() +"]");
        String uri = createUri();
        uri +="/commits?since=" + ServiceUtil.getIsoDateString(since);
        List<ResponseEntity<Object[]>> pagedCommits = new ArrayList<ResponseEntity<Object[]>>();

        System.out.println("Requesting with Uri: " + uri);

        HttpHeaders headers = ServiceUtil.createHeadersWithCredentials(this.user);
        HttpEntity<String> request = new HttpEntity<String>("",headers);

        ResponseEntity<Object[]> responseEntity = restTemplate.exchange(uri,HttpMethod.GET,request,Object[].class);
        pagedCommits.add(responseEntity);

        if(responseEntity.getHeaders().containsKey(HttpHeaders.LINK)){ //Link Header ist da, also mindestens 2 seiten
            boolean hasNext = true;
            pagedCommits.add(responseEntity);
            int page = 2;
            while (hasNext){
                uri = createUri();
                uri +="/commits?since=" + ServiceUtil.getIsoDateString(since) + "&page=" + page;

                ResponseEntity<Object[]> nextPage = restTemplate.exchange(uri,HttpMethod.GET,request,Object[].class);
                pagedCommits.add(nextPage);

                //prüfen ob weiterer "next" link verfügbar ist
                if(nextPage.getHeaders().get(HttpHeaders.LINK).toString().contains("next")){
                    System.out.println("Weitere Seite verfügbar");
                    hasNext = true;
                    page ++;
                }else{
                    System.out.println("Keine weiteren Seiten");
                    hasNext = false;
                }
            }
        }
        return pagedCommits;
    }


    private String createUri(){
        return URI + "/" + this.repoId.getOwner() + "/" + this.repoId.getName();
    }


}
