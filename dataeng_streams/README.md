#Streaming part of dataeng project

## Prerequisites
Java 11\
Maven\
Docker\
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

## Create connectors
```shell
curl -X POST -H "Content-Type:application/json" -d @configs/kym-http-connector-config.json http://localhost:8083/connectors
```

```shell
curl -X POST -H "Content-Type:application/json" -d @configs/spotlight-http-connector-config.json http://localhost:8083/connectors
```

## Run stream
```shell
java -jar target/components/dataeng_streams-0.1.jar
```

## Check messages
Different topics: kym, spotlight, kym_cleaned, spotlight_cleaned
```shell
docker exec kafka kafka-console-consumer --topic spotlight --property print.key=true --from-beginning --bootstrap-server localhost:9092
```

## Stop

```shell
docker-compose down --rmi local
```
