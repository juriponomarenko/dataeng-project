package org.ut.cs.dataeng_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.function.KymConverter;
import org.ut.cs.dataeng_streams.function.KymFilter;
import org.ut.cs.dataeng_streams.function.SpotlightConverter;
import org.ut.cs.dataeng_streams.function.SpotlightFilter;
import org.ut.cs.dataeng_streams.model.TopicName;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {

    private static final Logger LOG = LoggerFactory.getLogger(Pipe.class);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dataeng-streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        buildFlat(builder);

//        new KymCleaner(builder).clean();
//        new SpotlightCleaner(builder).clean();

        final Topology topology = builder.build();

        LOG.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        addShutdownHook(streams, latch);

        startStreamAndWaitOrExitOnFailure(streams, latch);

        System.exit(0);
    }

    private static void buildFlat(StreamsBuilder builder){
        KStream<String,String> stream = builder.stream(TopicName.KYM.getValue());
        KStream<String,String> filteredStream =  stream.filter(new KymFilter());
        KStream<String,String> mappedStream = filteredStream.map(new KymConverter());
        KGroupedStream<String,String> groupedStream = mappedStream.groupByKey();
        KTable<String, String> reducedTable = groupedStream.reduce((value1, value2) -> value2);

        // reducedTable.toStream().to(TopicName.KYM_CLEANED.getValue());
        //groupedStream.to(TopicName.KYM_CLEANED.getValue());

        KStream<String,String> spotlightStream = builder.stream(TopicName.SPOTLIGHT.getValue());
        KStream<String,String> spotlightFilteredStream = spotlightStream.filter(new SpotlightFilter());
        KStream<String,String> spotlightMappedStream = spotlightFilteredStream.map(new SpotlightConverter());
        KGroupedStream<String,String> spotlightGroupedStream = spotlightMappedStream.groupByKey();
        KTable<String,String> spotlightReducedTable = spotlightGroupedStream.reduce(((value1, value2) -> value2));

        KTable<String,String> joined = reducedTable.leftJoin(spotlightReducedTable, (value1, value2) -> "{\"kym\":" + value1 + ", \"spotlight\":" + value2 + "}");

        //spotlightMappedStream.to(TopicName.SPOTLIGHT_CLEANED.getValue());

        joined.toStream().to(TopicName.JOINED_STREAM.getValue());

    }

    private static void addShutdownHook(KafkaStreams streams, CountDownLatch latch) {
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
    }

    private static void startStreamAndWaitOrExitOnFailure(KafkaStreams streams, CountDownLatch latch) {
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            LOG.error("Streams execution failed. exiting", e);
            System.exit(1);
        }
    }

}
