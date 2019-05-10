package org.janelia.messaging.utils;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Created by schauderd on 1/18/18.
 */
public class MessagingUtils {
    public static Boolean getHeaderAsBoolean(Map<String, Object> headers, String header) {
        String headerValue = getHeaderAsString(headers, header);
        if (StringUtils.isNotBlank(headerValue)) {
            return Boolean.valueOf(headerValue);
        } else {
            return null;
        }
    }

    public static Long getHeaderAsLong(Map<String, Object> headers, String header) {
        String headerValue = getHeaderAsString(headers, header);
        if (StringUtils.isNotBlank(headerValue)) {
            return Long.valueOf(headerValue);
        } else {
            return null;
        }
    }

    public static String getHeaderAsString(Map<String, Object> headers, String header) {
        if (headers == null) {
            return null;
        } else {
            return valueAsString(headers.get(header));
        }
    }

    public static String valueAsString(Object data) {
        if (data != null) {
            return LongStringHelper.asLongString(((LongString)data).getBytes()).toString();
        } else {
            return null;
        }
    }

}
