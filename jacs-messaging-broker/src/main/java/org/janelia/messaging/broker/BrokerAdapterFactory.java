package org.janelia.messaging.broker;

import org.janelia.messaging.config.ApplicationConfig;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public abstract class BrokerAdapterFactory<T extends BrokerAdapter> {

    public BrokerAdapterArgs getBrokerAdapterArgs(ApplicationConfig config) {
        return new BrokerAdapterArgs(
                getName(),
                config.asMap().entrySet().stream()
                .filter(ce -> ce.getKey().startsWith(getName() + "."))
                .collect(Collectors.toMap(ce -> ce.getKey().substring(getName().length() + 1), ce -> ce.getValue()))
        );
    }

    /**
     * Get broker's name.
     * @return
     */
    @Nonnull
    public abstract String getName();

    /**
     * Create a broker adapter.
     *
     * @return
     */
    public abstract T createBrokerAdapter(BrokerAdapterArgs brokerAdapterArgs);
}
