package org.janelia.it.messaging.client;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by schauderd on 11/2/17.
 */
public class Receiver {
    Channel channel;
    ConnectionManager connectionManager;
    String queue;

    public Receiver() {

    }

    public void init(ConnectionManager pm, String bindingName, boolean temporary) {
        this.connectionManager = pm;

        // get a channel from the connectionManager
        try {
            channel = connectionManager.getConnection();
            
            // if no queue defined, get random queue and bind to this exchange
            if (temporary) {
                this.queue = channel.queueDeclare().getQueue();
                channel.queueBind(this.queue, bindingName, "");
            } else {
                this.queue = bindingName;
            }
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

    public void setupReceiver (Object handler) throws Exception {
        channel.basicConsume(queue,
                (consumerTag, delivery) -> ((DeliverCallback)handler).handle(consumerTag, delivery),
                (consumerTag) -> ((CancelCallback)handler).handle(consumerTag));

    }
}
