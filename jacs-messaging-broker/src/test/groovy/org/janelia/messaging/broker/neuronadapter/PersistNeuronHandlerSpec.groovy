package org.janelia.messaging.broker.neuronadapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.impl.LongStringHelper
import org.janelia.messaging.core.impl.MessageSenderImpl
import org.janelia.model.domain.Reference
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata
import spock.lang.Specification

class PersistNeuronHandlerSpec extends Specification {

    def user1Neuron
    def user4Neuron
    def systemOwnedNeuron

    ObjectMapper mapper
    TiledMicroscopeDomainMgr domainMgr
    MessageSenderImpl replySuccessSender
    MessageSenderImpl replyErrorSender
    def persistNeuronHandler

    def setup() {
        mapper = new ObjectMapper();

        user1Neuron = new TmNeuronMetadata();
        user1Neuron.name = "Neuron 1";
        user1Neuron.ownerKey = "user:testuser1";
        user1Neuron.writers = ["user:testuser1"];
        user1Neuron.readers = ["user:testuser1"];
        user1Neuron.setId(2468630633941827729);
        user1Neuron.setWorkspaceRef(Reference.createFor("TmWorkspace", 2463496977254449297));

        user4Neuron = new TmNeuronMetadata();
        user4Neuron.name = "Neuron 4";
        user4Neuron.ownerKey = "user:testuser1";
        user4Neuron.writers = ["user:testuser4","user:testuser1"];
        user4Neuron.readers = ["user:testuser4","user:testuser1"];
        user4Neuron.setId(2468630633941827729);
        user4Neuron.setWorkspaceRef(Reference.createFor("TmWorkspace", 2463496977254449297));

        systemOwnedNeuron = new TmNeuronMetadata();
        systemOwnedNeuron.name = "System Owned";
        systemOwnedNeuron.ownerKey = "group:mouselight";
        systemOwnedNeuron.writers = ["user:testuser2"];
        systemOwnedNeuron.readers = ["user:testuser2"];
        systemOwnedNeuron.setId(2468630633941827729);
        systemOwnedNeuron.setWorkspaceRef(Reference.createFor("TmWorkspace", 2463496977254449297));

        domainMgr = Mock(TiledMicroscopeDomainMgr.class);
        replySuccessSender = Mock(MessageSenderImpl.class);
        replyErrorSender = Mock(MessageSenderImpl.class);
        persistNeuronHandler = new PersistNeuronHandler(
                domainMgr,
                "group:mouselight",
                { Map<String, Object> messageHeaders, byte[] messageBody ->
                    replySuccessSender.sendMessage(messageHeaders, messageBody);
                },
                { Map<String, Object> messageHeaders, byte[] messageBody ->
                    // the error handler broadcasts it to all "known" senders
                    replySuccessSender.sendMessage(messageHeaders, messageBody);
                    replyErrorSender.sendMessage(messageHeaders, messageBody);
                }
        );
    }

    def "save neuron with body"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA")
        ]
        def msgBody = mapper.writeValueAsBytes(user1Neuron);

        and:
        domainMgr.saveMetadata(_, _) >> user1Neuron

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

        then:
        1 * domainMgr.saveMetadata(_, _)
        1 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "nothing happens when neuron owner and user are different"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:otheruser"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_SAVE_NEURONDATA")
        ]
        def msgBody = mapper.writeValueAsBytes(user1Neuron);

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

        then:
        0 * domainMgr.saveMetadata(_, _)
        0 * replySuccessSender.sendMessage(_, _)
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "delete neuron"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.NEURONIDS): ["2468630633941827729"],
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("NEURON_DELETE")
        ]
        def msgBody = mapper.writeValueAsBytes(user1Neuron);

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

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
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("true")
        ]
        def msgBody = mapper.writeValueAsBytes(user1Neuron);
        domainMgr.retrieve(_, _, "user:testuser2") >> [user1Neuron]

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

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
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("false")
        ]
        def msgBody = mapper.writeValueAsBytes(user1Neuron);
        domainMgr.retrieve(_, _, "user:testuser2") >> [user1Neuron]

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

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
                (NeuronMessageHeaders.USER)       : LongStringHelper.asLongString("user:testuser1"),
                (NeuronMessageHeaders.TARGET_USER): LongStringHelper.asLongString("user:testuser4"),
                (NeuronMessageHeaders.NEURONIDS)  : LongStringHelper.asLongString("2468630633941827729"),
                (NeuronMessageHeaders.WORKSPACE)  : LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)       : LongStringHelper.asLongString("REQUEST_NEURON_ASSIGNMENT"),
                (NeuronMessageHeaders.DECISION)   : LongStringHelper.asLongString("true")
        ]
        def msgBody =  mapper.writeValueAsBytes(user1Neuron);
        domainMgr.saveMetadata(_,_) >> { args -> args[0] }

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[NeuronMessageHeaders.TYPE] == "NEURON_OWNERSHIP_DECISION"
            def newNeuronValue = mapper.readValue(arguments[1], TmNeuronMetadata.class);
            assert newNeuronValue.ownerKey == "user:testuser4"
            assert newNeuronValue.readers.size() == 2
            assert newNeuronValue.writers.size() == 2
        }
        0 * replyErrorSender.sendMessage(_, _)
    }

    def "request ownership of a system neuron"() {
        given:
        def msgHeader = [
                (NeuronMessageHeaders.USER)     : LongStringHelper.asLongString("user:testuser2"),
                (NeuronMessageHeaders.NEURONIDS): LongStringHelper.asLongString("2468630633941827729"),
                (NeuronMessageHeaders.WORKSPACE): LongStringHelper.asLongString("2463496977254449297"),
                (NeuronMessageHeaders.TYPE)     : LongStringHelper.asLongString("REQUEST_NEURON_OWNERSHIP"),
                (NeuronMessageHeaders.DECISION) : LongStringHelper.asLongString("true")
        ]
        def msgBody = mapper.writeValueAsBytes(systemOwnedNeuron);
        domainMgr.retrieve(_, _, "user:testuser2") >> [systemOwnedNeuron]
        domainMgr.saveMetadata(_,_) >> { args -> args[0] }

        when:
        persistNeuronHandler.handleMessage(msgHeader, msgBody)

        then: "broadcast rejection message sent out"
        1 * replySuccessSender.sendMessage(*_) >> { arguments ->
            final Map<String,Object> msgHeaders = arguments[0]
            assert msgHeaders[NeuronMessageHeaders.TYPE] == "NEURON_OWNERSHIP_DECISION"
            def newNeuronValue = mapper.readValue(arguments[1], TmNeuronMetadata.class);
            assert newNeuronValue.ownerKey == "user:testuser2"
            assert newNeuronValue.readers.size() == 1
            assert newNeuronValue.writers.size() == 1
        }
        0 * replyErrorSender.sendMessage(_, _)
    }
}