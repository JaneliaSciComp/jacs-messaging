package org.janelia.it.messaging.broker.sharedworkspace

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.it.messaging.client.Sender
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

/**
 * Created by testuser1 on 12/1/17.
 */
class NeuronBrokerCRUDSpec extends Specification {
    def neuronBroker
    def broadcastRefreshSender
    def metadataStr = "{\"class\":\"org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata\",\"name\":\"Neuron 4\",\"ownerKey\":\"user:testuser1\",\"readers\":[\"user:testuser1\"],\"writers\":[\"user:testuser1\"],\"creationDate\":\"2017-11-09T22:43:28Z\",\"updatedDate\":\"2017-11-16T18:32:21Z\",\"workspaceRef\":\"TmWorkspace#2463496977254449297\",\"visible\":true,\"colorHex\":null,\"tags\":[],\"_id\":2468630633941827729}"
    def metadataObj

    def setup() {
        broadcastRefreshSender = Mock(Sender.class)
        neuronBroker = new NeuronBroker()
        neuronBroker.setBroadcastRefreshSender(broadcastRefreshSender)
    }

    def "save neuron data"() {
        setup:
        def domainMgr = Stub(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def metadataObj
        ObjectMapper mapper = new ObjectMapper();
        metadataObj = mapper.readValue(metadataStr, TmNeuronMetadata.class);
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (HeaderConstants.NEURONIDS) : ["2468630633941827729"],
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString(metadataStr)
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody
        domainMgr.save(_,_,_) >> metadataObj

        when:
        neuronBroker.handle("", testMessage)

        then:
        1 * broadcastRefreshSender.sendMessage(_, _)
    }

    def "save neuron metadata"() {
        setup:
        def domainMgr = Mock(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (HeaderConstants.NEURONIDS) : "2468630633941827729",
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("NEURON_SAVE_METADATA"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString("{\"class\":\"org.janelia.it.jacs.model.domain.tiledMicroscope.TmNeuronMetadata\",\"name\":\"Neuron 4\",\"ownerKey\":\"user:testuser1\",\"readers\":[\"user:testuser1\"],\"writers\":[\"user:testuser1\"],\"creationDate\":\"2017-11-09T22:43:28Z\",\"updatedDate\":\"2017-11-16T18:32:21Z\",\"workspaceRef\":\"TmWorkspace#2463496977254449297\",\"visible\":true,\"colorHex\":null,\"tags\":[],\"_id\":2468630633941827729}")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        when:
        neuronBroker.handle("", testMessage)

        then:
        1 * domainMgr.saveMetadata(_,"user:testuser1")
        1 * broadcastRefreshSender.sendMessage(_, _)
    }

    def "delete neuron"() {
        setup:
        def domainMgr = Mock(TiledMicroscopeDomainMgr.class)
        neuronBroker.setDomainMgr(domainMgr)
        def msgHeader = [ (HeaderConstants.USER) : LongStringHelper.asLongString("user:testuser1"),
                          (HeaderConstants.NEURONIDS) : ["2468630633941827729"],
                          (HeaderConstants.WORKSPACE) : LongStringHelper.asLongString("2463496977254449297"),
                          (HeaderConstants.TYPE) : LongStringHelper.asLongString("NEURON_DELETE"),
                          (HeaderConstants.METADATA) : LongStringHelper.asLongString("{\"class\":\"org.janelia.it.jacs.model.domain.tiledMicroscope.TmNeuronMetadata\",\"name\":\"Neuron 4\",\"ownerKey\":\"user:testuser1\",\"readers\":[\"user:testuser1\"],\"writers\":[\"user:testuser1\"],\"creationDate\":\"2017-11-09T22:43:28Z\",\"updatedDate\":\"2017-11-16T18:32:21Z\",\"workspaceRef\":\"TmWorkspace#2463496977254449297\",\"visible\":true,\"colorHex\":null,\"tags\":[],\"_id\":2468630633941827729}")
        ]
        def msgBody = "This is the message body".bytes
        def properties = Stub(AMQP.BasicProperties)
        properties.getHeaders() >> msgHeader
        def testMessage = Stub(Delivery.class)
        testMessage.getProperties() >> properties
        testMessage.getBody() >> msgBody

        when:
        neuronBroker.handle("", testMessage)

        then:
        1 * domainMgr.remove(_,"user:testuser1")
        1 * broadcastRefreshSender.sendMessage(_, _)
    }


}