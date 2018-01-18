package org.janelia.messaging.utility;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;

/**
 * Created by schauderd on 1/18/18.
 */
public class UtilityMethods {
    public static String convertLongString (LongString data) {
        if (data!=null)
            return LongStringHelper.asLongString(data.getBytes()).toString();
        return null;
    }
}
