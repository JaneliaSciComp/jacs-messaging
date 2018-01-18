package org.janelia.messaging.broker.sharedworkspace

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.messaging.client.Sender
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

/**
 * Created by schauderd on 12/1/17.
 * Integration testing assuming existing RabbitMQ server
 */
class NeuronBrokerOwnershipSpec extends Specification {
    def neuronBroker
    def broadcastRefreshSender
    def systemNeuron = "{\"class\":\"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata\",\"name\":\"Neuron 4\",\"ownerKey\":\"group:mouselight\",\"readers\":[\"user:mluser\"],\"writers\":[\"user:mluser\"],\"creationDate\":\"2017-11-09T22:43:28Z\",\"updatedDate\":\"2017-11-16T18:32:21Z\",\"workspaceRef\":\"TmWorkspace#2463496977254449297\",\"visible\":true,\"colorHex\":null,\"tags\":[],\"_id\":2468630633941827729}"
    def user1Neuron = "{\"class\":\"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata\",\"name\":\"Neuron 4\",\"ownerKey\":\"user:testuser1\",\"readers\":[\"user:testuser1\"],\"writers\":[\"user:testuser1\"],\"creationDate\":\"2017-11-09T22:43:28Z\",\"updatedDate\":\"2017-11-16T18:32:21Z\",\"workspaceRef\":\"TmWorkspace#2463496977254449297\",\"visible\":true,\"colorHex\":null,\"tags\":[],\"_id\":2468630633941827729}"
    def metadataObj

    def setup() {
        broadcastRefreshSender = Mock(Sender.class)
        neuronBroker = new NeuronBroker()
        neuronBroker.setBroadcastRefreshSender(broadcastRefreshSender)
    }

    def "requesting ownership of system neuron"() {
        setup:
        def domainMgr = Spy(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def metadataObj
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(systemNeuron, TmNeuronMetadata.class);
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser2"),
                          (HeaderConstants.NEURONIDS) : LongStringHelper.asLongString("2468630633941827729"),
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("REQUEST_NEURON_OWNERSHIP"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString(systemNeuron)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        neuronBroker.handle("", testMessage)

        then: "broadcast approval message sent out and neuron ownership updated"
        1 * broadcastRefreshSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.DESCRIPTION] == "Ownership approved by Neuron Owner"
        }
        1 * domainMgr.saveMetadata(*_) >> { arguments ->
            final TmNeuronMetadata neuron = arguments[0]
            assert neuron.getOwnerKey() == "user:testuser2"
        }
    }

    /**
     * commenting out for now, since in release 1 we aren't doing other owner requests
     *
    def "requesting ownership of another user's neuron"() {
        setup:
        def domainMgr = Stub(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def metadataObj
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(user1Neuron, TmNeuronMetadata.class);
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser2"),
                          (HeaderConstants.NEURONIDS) : LongStringHelper.asLongString("2468630633941827729"),
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("REQUEST_NEURON_OWNERSHIP"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString(systemNeuron)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        neuronBroker.handle("", testMessage)

        then: "broadcast message asking user for approval will be sent"
        1 * broadcastRefreshSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            print msgHeaders[HeaderConstants.TYPE]
            assert msgHeaders[HeaderConstants.TYPE] == "REQUEST_NEURON_OWNERSHIP"
        }
    }

    def "approval from owner of neuron to change ownership"() {
        setup:
        def domainMgr = Spy(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def metadataObj
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(systemNeuron, TmNeuronMetadata.class);
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser2"),
                          (HeaderConstants.NEURONIDS) : LongStringHelper.asLongString("2468630633941827729"),
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString(systemNeuron),
                          (HeaderConstants.DECISION) : LongStringHelper.asLongString("true")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        neuronBroker.handle("", testMessage)

        then: "broadcast approval message sent out and neuron ownership updated"
        1 * broadcastRefreshSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.DESCRIPTION] == "Ownership approved by Neuron Owner"
        }
        1 * domainMgr.saveMetadata(*_) >> { arguments ->
            final TmNeuronMetadata neuron = arguments[0]
            assert neuron.getOwnerKey() == "user:testuser2"
        }
    }

    def "rejection from owner of neuron to change ownership"() {
        setup:
        def domainMgr = Stub(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def metadataObj
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(systemNeuron, TmNeuronMetadata.class);
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser2"),
                          (HeaderConstants.NEURONIDS) : LongStringHelper.asLongString("2468630633941827729"),
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("NEURON_OWNERSHIP_DECISION"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString(systemNeuron),
                          (HeaderConstants.DECISION) : LongStringHelper.asLongString("false")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.retrieve(_, "user:testuser2") >> [metadataObj]

        when:
        neuronBroker.handle("", testMessage)

        then: "broadcast rejection message sent out"
        1 * broadcastRefreshSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[HeaderConstants.DECISION] == "false"
        }
    }
     */

}