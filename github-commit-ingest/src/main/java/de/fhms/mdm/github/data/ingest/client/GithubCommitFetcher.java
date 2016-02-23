package de.fhms.mdm.github.data.ingest.client;

import de.fhms.mdm.github.data.ingest.bean.GithubRepoId;
import de.fhms.mdm.github.data.ingest.bean.GithubUser;
import de.fhms.mdm.github.data.ingest.rest.service.GithubCommitService;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.*;

public class GithubCommitFetcher extends TableMapper<RepositoryWritable,Text> {
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";
    private static final String CLIENT_USER = "schleusenfrosch";

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        System.out.println("################## Map job GithubCommitFetcher ###########################");
        Map<String,String[]> repositories= new HashMap(); //repository name, String[] etag;modified-since
        String owner = Bytes.toString(row.get()); //kutzilla

        repositories = getValueHashMap(value);
        System.setProperty("http.proxyHost","10.60.17.102");
        System.setProperty("http.proxyPort","8080");
        System.setProperty("https.proxyHost","10.60.17.102");
        System.setProperty("https.proxyPort","8080");

        Set<String> keySet = repositories.keySet();
        for (String key : keySet) {
            String repositoryId = key;
            String lastEtag = repositories.get(key)[0];
            String lastModifiedSince = repositories.get(key)[1];

            final GithubRepoId githubRepoId = new GithubRepoId(owner, repositoryId);
            final GithubUser githubUser = new GithubUser(API_TOKEN, CLIENT_USER);
            GithubCommitService githubCommitService = new GithubCommitService(githubUser, githubRepoId, lastEtag);

            Date date = null;
            Calendar c = Calendar.getInstance();
            c.set(2016,1,1);
            if(lastModifiedSince.equals("none") && lastEtag.equals("none")) { //erster Pull und kein Datum angegeben
                date = c.getTime();
            }else{
                date = new Date(Long.parseLong(lastModifiedSince));
            }

            if (githubCommitService.areNewCommitsAvailableDate(date)) { //Neue Commits da?
                System.out.println("New Commits available for owner/Repo" + "[" + owner + "/" + repositoryId + "]");

                if(date != null) {
                    List<ResponseEntity<Object[]>> pagedCommits = githubCommitService.getCommitsSince(date);
                    String newEtag = pagedCommits.get(0).getHeaders().getETag();
                    Date newLastModified = new Date(pagedCommits.get(0).getHeaders().getLastModified());

                    System.out.println("New Etag: " + newEtag + "| new Date: " + newLastModified.getTime());

                    for (ResponseEntity<Object[]> page:pagedCommits) {
                        JSONArray array = new JSONArray(page.getBody());
                        for (int i = 0; i< array.length(); i++){
                            RepositoryWritable tmp = new RepositoryWritable();
                            tmp.setOwner(new Text(owner));
                            tmp.seteTag(new Text(newEtag));
                            tmp.setRepo(new Text(repositoryId));
                            tmp.setLastModifiedSince( new Text(String.valueOf(newLastModified.getTime())));//als Long persistieren
                            JSONObject wholeCommit = (JSONObject) array.get(i);
                            if(wholeCommit.has("committer")) { //falls Committer Tag vorhanden ist, kann auch null sein
                                JSONObject committer = (JSONObject) wholeCommit.get("committer");
                                String login = committer.getString("login");
                                tmp.setCommitter(new Text(login));
                                context.write(tmp, new Text(array.get(i).toString()));
                            }
                        }
                    }
                }
            }
        }
    }

    private Map<String,String[]> getValueHashMap(Result value){
        HashMap<String,String[]> repositories = new HashMap<String, String[]>();

        NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map =value.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long,
                byte[]>>> columnFamilyEntry : map.entrySet())
        {
            NavigableMap<byte[],NavigableMap<Long,byte[]>> columnMap = columnFamilyEntry.getValue();
            for( Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet())
            {
                NavigableMap<Long,byte[]> cellMap = columnEntry.getValue();
                for ( Map.Entry<Long, byte[]> cellEntry : cellMap.entrySet())
                {
                    System.out.println(String.format("Key : %s, Value :%s", Bytes.toString(columnEntry.getKey()),Bytes.toString(cellEntry.getValue())));
                    String[] values = Bytes.toString(cellEntry.getValue()).split(";"); //etag; modified-since
                    repositories.put(Bytes.toString(columnEntry.getKey()),values);

                }

            }
        }
        return repositories;
    }

}