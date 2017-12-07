package org.janelia.it.messaging.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
public class Sender {
    Channel channel;
    ConnectionManager connectionManager;
    String exchange;
    String routingKey;

    public Sender() {
    }

    public void init(ConnectionManager pm, String exchange, String routingKey) {
        this.connectionManager = pm;
        this.exchange = exchange;
        this.routingKey = routingKey;

        // get a channel from the connectionManager
        try {
            channel = connectionManager.getConnection();
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

    public void sendMessage(Map<String,Object> messageHeaders, byte[] messageBody) throws Exception {
        channel.basicPublish(exchange, routingKey,
                new AMQP.BasicProperties.Builder()
                        .headers(messageHeaders)
                        .build(), messageBody);
        System.out.println(" Message Sent to " + exchange +" with routingKey: " + routingKey);
    }

}
