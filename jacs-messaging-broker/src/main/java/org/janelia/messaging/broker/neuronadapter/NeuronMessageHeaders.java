package org.janelia.messaging.broker.neuronadapter;

/**
 * Created by schauderd on 11/14/17.
 *
 * Note:
 * Any changes in this class must propagate to the applications
 * that consume or produce neuron handling events.
 */
public class NeuronMessageHeaders {
    public static final String USER = "user";
    public static final String TARGET_USER = "target_user";
    public static final String NEURONIDS = "targetIds";
    public static final String WORKSPACE = "workspace";
    public static final String TYPE = "msgType";
    public static final String METADATA = "metadata";
    public static final String DECISION = "decision";
    public static final String DESCRIPTION = "description";
    public static final String OPERATION = "operation";
    public static final String TIMESTAMP = "timestamp";
}
