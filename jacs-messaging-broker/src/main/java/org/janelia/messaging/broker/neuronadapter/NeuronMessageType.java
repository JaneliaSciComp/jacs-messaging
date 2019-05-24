package org.janelia.messaging.broker.neuronadapter;

/**
 * Created by schauderd on 11/13/17.
 *
 * Note:
 * Any changes in this class must propagate to the applications
 * that consume or produce neuron handling events.
 */
public enum NeuronMessageType {
    NEURON_CREATE,
    NEURON_SAVE_NEURONDATA,
    NEURON_SAVE_METADATA,
    NEURON_DELETE,
    REQUEST_NEURON_OWNERSHIP,
    REQUEST_NEURON_ASSIGNMENT,
    NEURON_OWNERSHIP_DECISION,
    ERROR_PROCESSING
}
