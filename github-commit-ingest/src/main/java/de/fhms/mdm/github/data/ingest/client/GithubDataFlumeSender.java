package de.fhms.mdm.github.data.ingest.client;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GithubDataFlumeSender extends Reducer<RepositoryWritable,Text,Text,Text> {
    private static final String COMMITEVENT_TYPE = "CommitEvent";
    private static final String USERDATAEVENT_TYPE = "UserData";
    private static final String HOST = "localhost";
    private static final int PORT = 41414;

    @Override
    public void reduce(RepositoryWritable key, Iterable<Text> value, Context context
    ) throws IOException, InterruptedException {
        System.out.println("##################### Reducer ####################\n");
        List<Event> commitEvents = new ArrayList<Event>();
        String owner = key.getOwner().toString();
        String repository = key.getRepo().toString();
        String committer = key.getCommitter().toString();

        System.out.println("Committer:" + committer);

        for (Text data: value) {
            // Create a Flume Event object that encapsulates the sample data
            org.apache.flume.Event flumeEvent = EventBuilder.withBody(data.toString(), Charset.forName("UTF-8"));
            Map<String,String> headers = new HashMap<String,String>();
            headers.put(FlumeClientFacade.HEADER_EVENTTYPE,COMMITEVENT_TYPE);
            headers.put(FlumeClientFacade.HEADER_OWNER_NAME,owner);
            headers.put(FlumeClientFacade.HEADER_REPOSITORY_NAME,repository);
            headers.put(FlumeClientFacade.HEADER_COMMITTER_NAME,committer);
            flumeEvent.setHeaders(headers);
            commitEvents.add(flumeEvent);
        }

        //User Data Event bauen
        Event userDataEvent = EventBuilder.withBody(committer.toString(),Charset.forName("UTF-8"));
        Map<String,String> headers = new HashMap<String,String>();
        headers.put(FlumeClientFacade.HEADER_EVENTTYPE,USERDATAEVENT_TYPE);
        userDataEvent.setHeaders(headers);

        commitEvents.add(userDataEvent);

        final FlumeClientFacade client = new FlumeClientFacade();
        //senden des Flume Events
        client.init(HOST, PORT);
        client.sendDataToFlume(commitEvents);
        client.cleanUp();

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf("Repositories"));
        try {
            Put p = new Put(Bytes.toBytes(owner)); //zeile
            String cellValue = key.geteTag() + ";" + key.getLastModifiedSince();
            p.add(Bytes.toBytes("repos"),Bytes.toBytes(repository),Bytes.toBytes(cellValue));//Column family und wert
            table.put(p); //wegschreiben
        }finally {
            connection.close();
        }
    }
}