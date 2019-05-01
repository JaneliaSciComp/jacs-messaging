package org.janelia.messaging.utils;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;

/**
 * Created by schauderd on 1/18/18.
 */
public class MessagingUtils {
    public static String convertLongString(LongString data) {
        if (data != null) {
            return LongStringHelper.asLongString(data.getBytes()).toString();
        } else {
            return null;
        }
    }
}
