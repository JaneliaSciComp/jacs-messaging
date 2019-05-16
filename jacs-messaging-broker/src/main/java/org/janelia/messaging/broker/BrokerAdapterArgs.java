package org.janelia.messaging.broker;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class BrokerAdapterArgs {
    private static final long DEFAULT_BACKUP_INTERVAL_IN_MILLIS = 86400000L;

    private final String adapterName;
    private final Map<String, String> brokerAdapterConfig;

    BrokerAdapterArgs(String adapterName, Map<String, String> brokerAdapterConfig) {
        this.adapterName = adapterName;
        this.brokerAdapterConfig = brokerAdapterConfig;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public String getAdapterConfig(String configProperty) {
        return brokerAdapterConfig.get(configProperty);
    }

    public String getAdapterConfigOrDefault(String configProperty, String defaultValue) {
        return StringUtils.defaultIfBlank(brokerAdapterConfig.get(configProperty), defaultValue);
    }

    String getReceiveQueue() {
        return getAdapterConfig("receiveQueue");
    }

    String getSuccessResponseExchange() {
        return getAdapterConfig("successResponseQueue");
    }

    String getSuccessResponseRouting() {
        return getAdapterConfigOrDefault("successResponseRouting", "");
    }

    String getErrorResponseExchange() {
        return getAdapterConfig("errorResponseQueue");
    }

    String getErrorResponseRouting() {
        return getAdapterConfigOrDefault("errorResponseRouting", "");
    }

    String getBackupQueue() {
        return getAdapterConfig("backupQueue");
    }

    Long getBackupIntervalInMillis() {
        String backupInterval = getAdapterConfig("backupIntervalInMillis");
        if (StringUtils.isNotBlank(backupInterval)) {
            return Long.valueOf(backupInterval.trim());
        } else {
            return DEFAULT_BACKUP_INTERVAL_IN_MILLIS;
        }
    }

    String getBackupLocation() {
        return getAdapterConfig("backupLocation");
    }

}
