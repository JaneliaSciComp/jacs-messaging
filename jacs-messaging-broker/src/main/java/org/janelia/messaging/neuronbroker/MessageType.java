package org.janelia.messaging.neuronbroker;

/**
 * Created by schauderd on 11/13/17.
 */
public enum MessageType {
    NEURON_CREATE,
    NEURON_SAVE_NEURONDATA,
    NEURON_SAVE_METADATA,
    NEURON_DELETE,
    REQUEST_NEURON_OWNERSHIP,
    REQUEST_NEURON_ASSIGNMENT,
    NEURON_OWNERSHIP_DECISION,
    ERROR_PROCESSING
}
