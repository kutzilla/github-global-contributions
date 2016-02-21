package de.fhms.mdm.github.data.ingest.client;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Dave on 04.02.2016.
 */
public class FlumeClientFacade {
    private static final Logger LOGGER = Logger.getLogger(FlumeClientFacade.class.getSimpleName());
    public static final String HEADER_EVENTTYPE = "EventType";
    public static final String HEADER_OWNER_NAME = "OwnerName";
    public static final String HEADER_REPOSITORY_NAME = "RepositoryName";
    public static final String HEADER_COMMITTER_NAME = "Committer";

    private RpcClient client;
    private String hostname;
    private int port;

    public void init(String hostname, int port) {
        LOGGER.log(Level.INFO,"Initialisiere RPC-Client");
        // Setup the RPC connection
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);

    }

    public void sendDataToFlume(List<org.apache.flume.Event> flumeEvents) {
        // Send the event
        try {
            client.appendBatch(flumeEvents);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
        }
    }

    public void cleanUp() {
        client.close();
    }

 }
