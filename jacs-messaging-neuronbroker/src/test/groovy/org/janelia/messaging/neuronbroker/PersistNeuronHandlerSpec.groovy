package org.janelia.messaging.neuronbroker

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.messaging.core.MessageSender
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

class PersistNeuronHandlerSpec extends Specification {

    def metadataStr = "{" +
            "\"class\":\"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata\"," +
            "\"name\":\"Neuron 4\"," +
            "\"ownerKey\":" +
            "\"user:testuser1\"," +
            "\"readers\":[\"user:testuser1\"]," +
            "\"writers\":[\"user:testuser1\"]," +
            "\"creationDate\":\"2017-11-09T22:43:28Z\"," +
            "\"updatedDate\":\"2017-11-16T18:32:21Z\"," +
            "\"workspaceRef\":\"TmWorkspace#2463496977254449297\"," +
            "\"visible\":true," +
            "\"colorHex\":null," +
            "\"tags\":[]," +
            "\"_id\":2468630633941827729" +
            "}"
    TmNeuronMetadata metadataObj;
    TiledMicroscopeDomainMgr domainMgr;
    MessageSender replySuccessSender;
    MessageSender replyErrorSender;
    def persistNeuronHandler

    def setup() {
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(metadataStr, TmNeuronMetadata.class);

        domainMgr = Mock(TiledMicroscopeDomainMgr.class);
        replySuccessSender = Mock(MessageSender.class);
        replyErrorSender = Mock(MessageSender.class);
        persistNeuronHandler = new PersistNeuronHandler(
                domainMgr,
                "group:mouselight",
                replySuccessSender,
                replyErrorSender);
    }

    def "save neuron with body"() {
        given:
        def msgHeader = [ (NeuronMessageHeaders.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (NeuronMessageHeaders.NEURONIDS) : ["2468630633941827729"],
                          (NeuronMessageHeaders.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (NeuronMessageHeaders.TYPE) : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                          (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.save(_,_,_) >> metadataObj

        when:
        persistNeuronHandler.handle("", testMessage)

        then:
        1 * domainMgr.save(_, _, _)
        0 * domainMgr.saveMetadata(_, _)
        1 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "nothing happens when neuron owner and user are different"() {
        given:
        def msgHeader = [ (NeuronMessageHeaders.USER) : LongStringHelper.asLongString("user:otheruser"),
                          (NeuronMessageHeaders.NEURONIDS) : ["2468630633941827729"],
                          (NeuronMessageHeaders.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (NeuronMessageHeaders.TYPE) : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                          (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        when:
        persistNeuronHandler.handle("", testMessage)

        then:
        0 * domainMgr.save(_, _, _)
        0 * domainMgr.saveMetadata(_, _)
        0 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "save neuron metadata"() {
        given:
        def msgHeader = [ (NeuronMessageHeaders.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (NeuronMessageHeaders.NEURONIDS) : ["2468630633941827729"],
                          (NeuronMessageHeaders.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (NeuronMessageHeaders.TYPE) : LongStringHelper.asLongString("NEURON_SAVE_METADATA"),
                          (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.saveMetadata(_,_) >> metadataObj

        when:
        persistNeuronHandler.handle("", testMessage)

        then:
        0 * domainMgr.save(_, _, _)
        1 * domainMgr.saveMetadata(_, _)
        1 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "delete neuron"() {
        given:
        def msgHeader = [ (NeuronMessageHeaders.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (NeuronMessageHeaders.NEURONIDS) : ["2468630633941827729"],
                          (NeuronMessageHeaders.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (NeuronMessageHeaders.TYPE) : LongStringHelper.asLongString("NEURON_DELETE"),
                          (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        when:
        persistNeuronHandler.handle("", testMessage)

        then:
        1 * domainMgr.remove(_,"user:testuser1")
        1 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

}