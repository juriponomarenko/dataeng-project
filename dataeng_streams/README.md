# Streaming part of dataeng project

## Prerequisites

Java 11

Maven

Docker

git

## Install

Download the code
```shell
git clone https://github.com/juriponomarenko/dataeng-project.git
```

Move to streams directory
```shell
cd dataeng-project/dataeng_streams
```

## Build

```shell
mvn clean package
```

## Start services

```shell
docker-compose up -d
```
It might be necessary to include kafka in /etc/hosts file:

127.0.0.1 kafka

## Create connectors

Source connector for Know you meme dataset
```shell
curl -X POST -H "Content-Type:application/json" -d @configs/kym-http-connector-config.json http://localhost:8083/connectors
```

Source connector for Spotlight dataset
```shell
curl -X POST -H "Content-Type:application/json" -d @configs/spotlight-http-connector-config.json http://localhost:8083/connectors
```

JDBC sink connector for Postgres DB
```shell
curl -X POST -H "Content-Type:application/json" -d @configs/postgres-connector-config.json http://localhost:8083/connectors
```

## Run stream

```shell
java -jar target/components/dataeng_streams-0.1.jar
```

## Check messages

Different topics: kym, spotlight, joined_stream
```shell
docker exec kafka kafka-console-consumer --topic spotlight --property print.key=true --from-beginning --bootstrap-server localhost:9092
```

## Stop

```shell
docker-compose down --rmi local
```
