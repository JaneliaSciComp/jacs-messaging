# JACS Messaging

[![CircleCI](https://circleci.com/gh/JaneliaSciComp/jacs-messaging.svg?style=svg)](https://circleci.com/gh/JaneliaSciComp/jacs-messaging)

## Build 

Build the library jar (jacs-messaging-<vers>.jar):
`gradle build`

Build the Neuron Broker jar:
`gradle brokerJar`

Build the Backup Queue Utility jar:
`gradle queueBackupQueryJar`

Build the Neuron Recovery Utility jar:
`gradle userRecoveryJar`

## Manage/Deploy 

Start RabbitMQ on remote server:
`gradle startRabbit`

Stop RabbitMQ on remote server:
`gradle stopRabbit`

Deploy Neuron Broker to remote server:
`gradle deployNeuronBroker`

Stop Neuron Broker on remote server:
`gradle stopNeuronBroker`

## Summary

This module contains common classes for supporting clients performing asynchronous messaging in java using rabbitMQ, a popular and lightweight messaging framework.  

It also contains server components for the shared workspace feature in LVV/Horta, allowing realtime broadcasting of updates to all tracers in the same workspace.  Finally, there are some utilities for downloading message backup history and parsing the history to recover neuron information.

## License 

[Janelia Open Source License](https://www.janelia.org/open-science/software-licensing)

