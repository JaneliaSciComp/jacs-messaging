package org.janelia.messaging.neuronbroker

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.messaging.core.MessageSender
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

class PersistNeuronHandlerSpec extends Specification {

    def metadataStr = """
    {
        "class": "org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata",
        "name": "Neuron 4",
        "ownerKey": "user:testuser1",
        "readers": ["user:testuser1"],
        "writers": ["user:testuser1"],
        "creationDate": "2017-11-09T22:43:28Z",
        "updatedDate": "2017-11-16T18:32:21Z",
        "workspaceRef": "TmWorkspace#2463496977254449297",
        "visible": true,
        "colorHex": null,
        "tags": [],
        "_id": 2468630633941827729
    }
    """
    def user4Neuron = """
    {
        "class":"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata",
        "name":"Neuron 4",
        "ownerKey":"user:testuser4",
        "readers":["user:testuser4","user:testuser1"],
        "writers":["user:testuser4","user:testuser1"],
        "creationDate":"2017-11-09T22:43:28Z",
        "updatedDate":"2017-11-16T18:32:21Z",
        "workspaceRef":"TmWorkspace#2463496977254449297",
        "visible":true,
        "colorHex":null,
        "tags":[],
        "_id":2468630633941827729
    }
    """

    ObjectMapper mapper
    TmNeuronMetadata metadataObj
    TiledMicroscopeDomainMgr domainMgr
    MessageSender replySuccessSender
    MessageSender replyErrorSender
    def persistNeuronHandler

    def setup() {
        mapper = new ObjectMapper();
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
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.save(_, _, _) >> metadataObj

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
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:otheruser"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
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
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_METADATA"),
                (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.saveMetadata(_, _) >> metadataObj

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
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_DELETE"),
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
        1 * domainMgr.remove(_, "user:testuser1")
        1 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "approval from owner of neuron to change ownership"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (NeuronMessageHeaders.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr),
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("true")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast approval message sent out and neuron ownership updated"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String, Object> msgHeaders = arguments[0]
            assert msgHeaders[NeuronMessageHeaders.DESCRIPTION] == "Ownership approved by Neuron Owner"
        }
        1 * domainMgr.saveMetadata(*_) >> { arguments ->
            final TmNeuronMetadata neuron = arguments[0]
            assert neuron.getOwnerKey() == "user:testuser2"
            return neuron
        }
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "rejection from owner of neuron to change ownership"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (NeuronMessageHeaders.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr),
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("false")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[NeuronMessageHeaders.DECISION] == "false"
        }
        0 * domainMgr.saveMetadata(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "assign owner of neuron to another person"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (NeuronMessageHeaders.TARGET_USER) : LongStringHelper.asLongString("user:testuser4"),
                (NeuronMessageHeaders.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("REQUEST_NEURON_ASSIGNMENT"),
                (NeuronMessageHeaders.METADATA) : LongStringHelper.asLongString(metadataStr),
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("false")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        def neuronObj4 = mapper.readValue(user4Neuron, TmNeuronMetadata.class);
        domainMgr.saveMetadata(_,_) >> { args -> args[0] }
        domainMgr.setPermissions(_,_,_) >> { neuronObj4 }

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[NeuronMessageHeaders.TYPE] == "NEURON_OWNERSHIP_DECISION"
            def newNeuronValue = mapper.readValue(msgHeaders[NeuronMessageHeaders.METADATA], TmNeuronMetadata.class);
            assert newNeuronValue.ownerKey == "user:testuser4"
            assert newNeuronValue.readers.size() == 2
            assert newNeuronValue.writers.size() == 2
        }
        0 * replyErrorSender.sendMessage(_, _)
    }

}