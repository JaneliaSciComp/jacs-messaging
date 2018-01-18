package org.janelia.messaging.client;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import org.janelia.messaging.broker.sharedworkspace.HeaderConstants;
import org.janelia.messaging.broker.sharedworkspace.MessageType;
import org.janelia.messaging.utility.UtilityMethods;
import org.msgpack.jackson.dataformat.msgpack.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

/**
 * Created by schauderd on 1/18/18.
 * This class connects to a queue and completely drains it to a provided outputstream,
 * purging the queue after downloading all the messages
 */
public class BulkConsumer {
    private static final Logger log = LoggerFactory.getLogger(BulkConsumer.class);
    Channel channel;
    ConnectionManager connectionManager;
    String queue;
    boolean purgeOnCopy;

    public BulkConsumer() {

    }

    public void init(ConnectionManager pm, String bindingName) {
        this.connectionManager = pm;

        // get a channel from the connectionManager
        try {
            channel = connectionManager.getConnection();

            this.queue = bindingName;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanUp() {
        try {
            channel.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // problems closing out the
            e.printStackTrace();
        }
    }

    public int copyQueue (OutputStream stream) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<VanillaBean> backupMessages = new ArrayList<>();
        GetResponse message = channel.basicGet(queue, purgeOnCopy);
        int msgCount = 0;
        if (message!=null) {
             msgCount = message.getMessageCount();
        }
        while (message!=null && message.getMessageCount()>0) {
            VanillaBean bean = new VanillaBean(cleanUpHeaders(message.getProps().getHeaders()),
                    message.getBody());
            backupMessages.add(bean);
            message = channel.basicGet(queue, purgeOnCopy);
        }

        // process last message
        if (message!=null) {
            VanillaBean bean = new VanillaBean(cleanUpHeaders(message.getProps().getHeaders()), message.getBody());
            backupMessages.add(bean);
        }

        mapper.writeValue(stream, backupMessages);
        return msgCount;
    }

    private Map<String,Object> cleanUpHeaders (Map<String,Object> headers) {
        Map<String,Object> newHeaders = new HashMap<String,Object>();
        newHeaders.put(HeaderConstants.USER, UtilityMethods.convertLongString((LongString) headers.get(HeaderConstants.USER)));
        newHeaders.put(HeaderConstants.WORKSPACE, Long.parseLong(UtilityMethods.convertLongString((LongString) headers.get(HeaderConstants.WORKSPACE))));
        newHeaders.put(HeaderConstants.TYPE, MessageType.valueOf(UtilityMethods.convertLongString((LongString) headers.get(HeaderConstants.TYPE))));
        newHeaders.put(HeaderConstants.METADATA, UtilityMethods.convertLongString((LongString) headers.get(HeaderConstants.METADATA)));
        return newHeaders;
    }

    public boolean isPurgeOnCopy() {
        return purgeOnCopy;
    }

    public void setPurgeOnCopy(boolean purgeOnCopy) {
        this.purgeOnCopy = purgeOnCopy;
    }

    public class VanillaBean {
        byte[] body;
        Map<String,Object> headers;

        VanillaBean(Map<String,Object> headers, byte[] body) {
            this.headers = headers;
            this.body = body;
        }

        public Map<String, Object> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, Object> headers) {
            this.headers = headers;
        }

        public byte[] getBody() {
            return body;
        }

        public void setBody(byte[] body) {
            this.body = body;
        }
    }
}