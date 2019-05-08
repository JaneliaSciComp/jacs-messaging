package org.janelia.messaging.broker.neuronadapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.messaging.core.MessageSender
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

class PersistNeuronHandlerSpec extends Specification {

    def user1NeuronJson = """
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
    def user4NeuronJson = """
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
    def systemOwnedNeuronJson = """
    {
        "class":"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata",
        "name":"Neuron 4",
        "ownerKey":"group:mouselight",
        "readers":["user:testuser2"],
        "writers":["user:testuser2"],
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
    TiledMicroscopeDomainMgr domainMgr
    MessageSender replySuccessSender
    MessageSender replyErrorSender
    def persistNeuronHandler

    def setup() {
        mapper = new ObjectMapper();
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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (HeaderConstants.NEURONIDS): ["2468630633941827729"],
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson)
        ]
        def user1Neuron = mapper.readValue(user1NeuronJson, TmNeuronMetadata.class);
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.save(_, _, _) >> user1Neuron

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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:otheruser"),
                (HeaderConstants.NEURONIDS): ["2468630633941827729"],
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson)
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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (HeaderConstants.NEURONIDS): ["2468630633941827729"],
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_METADATA"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson)
        ]
        def user1Neuron = mapper.readValue(user1NeuronJson, TmNeuronMetadata.class);
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        and:
        domainMgr.saveMetadata(_, _) >> user1Neuron

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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (HeaderConstants.NEURONIDS): ["2468630633941827729"],
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_DELETE"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson)
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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (HeaderConstants.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson),
                (HeaderConstants.DECISION) : LongStringHelper.asLongString("true")
        ]
        def user1Neuron = mapper.readValue(user1NeuronJson, TmNeuronMetadata.class);
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [user1Neuron]

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast approval message sent out and neuron ownership updated"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String, Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.DESCRIPTION] == "Ownership approved by Neuron Owner"
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
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (HeaderConstants.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(user1NeuronJson),
                (HeaderConstants.DECISION) : LongStringHelper.asLongString("false")
        ]
        def user1Neuron = mapper.readValue(user1NeuronJson, TmNeuronMetadata.class);
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [user1Neuron]

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.DECISION] == "false"
        }
        0 * domainMgr.saveMetadata(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "assign owner of neuron to another person"() {
        given:
        def msgHeader = [
                (HeaderConstants.USER)       : LongStringHelper.asLongString("user:testuser2"),
                (HeaderConstants.TARGET_USER): LongStringHelper.asLongString("user:testuser4"),
                (HeaderConstants.NEURONIDS)  : LongStringHelper.asLongString("2468630633941827729"),
                (HeaderConstants.WORKSPACE)  : LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)       : LongStringHelper.asLongString("REQUEST_NEURON_ASSIGNMENT"),
                (HeaderConstants.METADATA)   : LongStringHelper.asLongString(user1NeuronJson),
                (HeaderConstants.DECISION)   : LongStringHelper.asLongString("false")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        def user4Neuron = mapper.readValue(user4NeuronJson, TmNeuronMetadata.class);
        domainMgr.saveMetadata(_,_) >> { args -> args[0] }
        domainMgr.setPermissions(_,_,_) >> { user4Neuron }

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.TYPE] == "NEURON_OWNERSHIP_DECISION"
            def newNeuronValue = mapper.readValue(msgHeaders[HeaderConstants.METADATA], TmNeuronMetadata.class);
            assert newNeuronValue.ownerKey == "user:testuser4"
            assert newNeuronValue.readers.size() == 2
            assert newNeuronValue.writers.size() == 2
        }
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "request ownership of a system neuron"() {
        given:
        def msgHeader = [
                (HeaderConstants.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (HeaderConstants.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (HeaderConstants.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (HeaderConstants.TYPE)     : LongStringHelper.asLongString("REQUEST_NEURON_OWNERSHIP"),
                (HeaderConstants.METADATA) : LongStringHelper.asLongString(systemOwnedNeuronJson),
                (HeaderConstants.DECISION) : LongStringHelper.asLongString("false")
        ]
        def systemOwnedNeuron = mapper.readValue(systemOwnedNeuronJson, TmNeuronMetadata.class);
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [systemOwnedNeuron]
        domainMgr.saveMetadata(_,_) >> { args -> args[0] }

        when:
        persistNeuronHandler.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.TYPE] == "NEURON_OWNERSHIP_DECISION"
            def newNeuronValue = mapper.readValue(msgHeaders[HeaderConstants.METADATA], TmNeuronMetadata.class);
            assert newNeuronValue.ownerKey == "user:testuser2"
            assert newNeuronValue.readers.size() == 1
            assert newNeuronValue.writers.size() == 1
        }
        0 * replyErrorSender.sendMessage(_, _)
    }
}