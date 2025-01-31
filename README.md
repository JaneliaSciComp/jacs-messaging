# JACS Messaging

[![Java CI with Maven](https://github.com/JaneliaSciComp/jacs-messaging/actions/workflows/gradle.yml/badge.svg)](https://github.com/JaneliaSciComp/jacs-messaging/actions/workflows/gradle.yml)
[![DOI](https://zenodo.org/badge/113470371.svg)](https://doi.org/10.5281/zenodo.14610033)

This module contains common classes for supporting clients performing asynchronous messaging in java using rabbitMQ, a popular and lightweight messaging framework.  

It also contains server components for the shared workspace feature in LVV/Horta, allowing realtime broadcasting of updates to all tracers in the same workspace.  Finally, there are some utilities for downloading message backup history and parsing the history to recover neuron information.

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

## Publish to maven repo
`gradle -PmavenRepoUser=YourUserName -PmavenRepoPassword=YourPassword publish`

## License 

[Janelia Open Source License](https://www.janelia.org/open-science/software-licensing)

