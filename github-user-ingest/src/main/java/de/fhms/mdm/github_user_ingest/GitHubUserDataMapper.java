package de.fhms.mdm.github_user_ingest;

import de.fhms.mdm.github_user_ingest.service.GitHubUserService;
import de.fhms.mdm.github_user_ingest.service.GithubUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.json.JSONObject;
import org.springframework.http.ResponseEntity;


import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Created by Dave on 22.02.2016.
 */
public class GitHubUserDataMapper extends Mapper<LongWritable,Text,NullOutputFormat,NullOutputFormat> {
    private static final String API_TOKEN = "56cb7372fefdee1cefac895658c2270ad039d18f";
    private static final String CLIENT_USER = "schleusenfrosch";
    private static final String HBASE_USERS_TABLE_NAME = "Users";
    private static final String HBASE_LOCATIONS_TABLE_NAME = "Locations";
    private static final String HBASE_MASTER = "hbase.master";
    private static final String HBASE_MASTER_HOST = "quickstart.cloudera:60010";

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        System.out.println("################## Map job GitHubUserDataMapper for " + value.toString() + " ###########################");

        String userLogin = value.toString().trim();
        String location = null;
        boolean validLocation = false;
        boolean userAlreadyExisted = false;

        //Proxy setzen, wenn auf Cluster VM ausgeführt
        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration<InetAddress> ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                if (i.getHostAddress().toString().equals("10.60.67.4")) {
                    System.out.println("########################### Setting Proxy ###########################");
                    System.setProperty("http.proxyHost", "10.60.17.102");
                    System.setProperty("http.proxyPort", "8080");
                    System.setProperty("https.proxyHost", "10.60.17.102");
                    System.setProperty("https.proxyPort", "8080");
                }
            }
        }


        GithubUser user = new GithubUser(API_TOKEN, CLIENT_USER);
        GitHubUserService gitHubUserService = new GitHubUserService(user);

        Connection connection = createConnection();

        Get get = new Get(Bytes.toBytes(userLogin)); //Get zum Abfragen, ob User bereits in Metatabelle
        Table usersTable = getTable(HBASE_USERS_TABLE_NAME,connection);
        Result result = usersTable.get(get);

        if(result.isEmpty()){//prüfen ob Eintrag bereits in der Meta Tabelle Users vorhanden
            System.out.println("New User:" + userLogin );
            ResponseEntity<String> responseEntity = gitHubUserService.getUser(userLogin);
            if(responseEntity!= null) {
                JSONObject userObject = new JSONObject(responseEntity.getBody());
                if(locationNotNull(userObject)){
                    location = userObject.getString("location");
                    validLocation = true;
                }
            }
        }else{ //Eintrag bereits in der Meta Tabelle, also kein Aufruf der Api
            System.out.println("User already existed:" + userLogin);
            location = Bytes.toString(result.getValue(Bytes.toBytes("user"),Bytes.toBytes("location")));
            userAlreadyExisted = true;
        }

        if(userAlreadyExisted) {
            if(validLocation) {
                saveLocation(location, connection);
            }

        }else{ //User war noch nicht in der Metadaten Tabelle
            if(validLocation) {
                saveUser(userLogin, location, connection);
                saveLocation(location, connection);
            }
        }

        connection.close();


    }
    //Daten in die Users Metatabelle schreiben
    private void saveUser(String userLogin,String location,Connection connection){
        System.out.println("Entering saveUser[" + userLogin + " " + location + "]");
        try {
            Table usersTable = getTable(HBASE_USERS_TABLE_NAME,connection);
            Put putUser = new Put(Bytes.toBytes(userLogin));
            putUser.add(Bytes.toBytes("user"), Bytes.toBytes("location"), Bytes.toBytes(location));//Column family und wert
            usersTable.put(putUser); //wegschreiben
        }catch (IOException ex) {
            System.out.println("Couldn't save User");
        }
    }

    //Daten in die Locations Metatabelle schreiben
    private void saveLocation(String location,Connection connection){
        System.out.println("Entering saveLocation[" + location + "]");
        try {
            Table locationsTable = getTable(HBASE_LOCATIONS_TABLE_NAME,connection);
            Get get = new Get(Bytes.toBytes(location));
            Result result = locationsTable.get(get);
            if(result != null) {//Location noch nicht in der Tabelle, also rein schreiben
                Put putUser = new Put(Bytes.toBytes(location));
                putUser.add(Bytes.toBytes("location"), Bytes.toBytes("longitude"), Bytes.toBytes("none"));//Column family und wert
                putUser.add(Bytes.toBytes("location"), Bytes.toBytes("latitude"), Bytes.toBytes("none"));//Column family und wert
                putUser.add(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes("none"));//Column family und wert
                putUser.add(Bytes.toBytes("location"), Bytes.toBytes("country"), Bytes.toBytes("none"));//Column family und wert
                locationsTable.put(putUser); //wegschreiben
            }
        }catch (IOException ex){
            System.out.println("Couldn't save Location");
        }

    }
    //Gibt Tabelle für speziellen Namen zurück
    private Table getTable(String tableName,Connection connection) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    private Connection createConnection(){
        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_MASTER,HBASE_MASTER_HOST);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);

        } catch (IOException e) {
            System.out.println("Couldn't create connection");
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * Prüft Ob Location String vorhanden und nicht NULL
     * @param user
     * @return
     */
    private boolean locationNotNull(JSONObject user){
        if (user.has("location")){
            if(!user.isNull("location")){
                return true;
            }
        }
        return false;
    }
}
