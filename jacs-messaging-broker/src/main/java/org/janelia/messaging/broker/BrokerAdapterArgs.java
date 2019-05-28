package org.janelia.messaging.broker;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

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

    public String getReceiveQueue() {
        return getAdapterConfig("receiveQueue");
    }

    public String getSuccessResponseExchange() {
        return getAdapterConfig("successResponseQueue");
    }

    public String getSuccessResponseRouting() {
        return getAdapterConfigOrDefault("successResponseRouting", "");
    }

    public String getErrorResponseExchange() {
        return getAdapterConfig("errorResponseQueue");
    }

    public String getErrorResponseRouting() {
        return getAdapterConfigOrDefault("errorResponseRouting", "");
    }

    public String getBackupQueue() {
        return getAdapterConfig("backupQueue");
    }

    public Long getBackupIntervalInMillis() {
        String backupInterval = getAdapterConfig("backupIntervalInMillis");
        if (StringUtils.isNotBlank(backupInterval)) {
            return Long.valueOf(backupInterval.trim());
        } else {
            return DEFAULT_BACKUP_INTERVAL_IN_MILLIS;
        }
    }

    public String getBackupLocation() {
        return getAdapterConfig("backupLocation");
    }

}
