package de.fhms.mdm.github_user_ingest.util;

import de.fhms.mdm.github_user_ingest.service.GithubUser;
import org.apache.commons.net.util.Base64;
import org.springframework.http.HttpHeaders;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Dave on 21.02.2016.
 */
public class ServiceUtil {

    public static String getIsoDateString(Date date){
        SimpleDateFormat isoFormat =  new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
        return isoFormat.format(date);

    }
    public static  HttpHeaders createHeadersWithCredentials(GithubUser user){
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, "Basic " + createCredentials(user));
        return headers;
    }
    public static  String createCredentials(GithubUser user){
        String plainCreds = user.getLogin() + ":" + user.getApiToken();
        byte[] plainCredsBytes = plainCreds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        return  new String(base64CredsBytes);
    }
}
