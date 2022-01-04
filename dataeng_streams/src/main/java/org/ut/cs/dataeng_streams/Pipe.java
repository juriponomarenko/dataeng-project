package org.ut.cs.dataeng_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        new KymCleaner(builder).clean();
        new SpotlightCleaner(builder).clean();

        //cleanKym(builder);
        //cleanSpotlight(builder);

//        KStream<String, String> kymStream = builder.stream(TopicName.KYM.getValue());
//        kymStream.to("kym-cleaned");
        //TODO more filtering: remove duplicates(title, url, last_update_source)
        //TODO title from url

        final Topology topology = builder.build();

        LOG.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        addShutdownHook(streams, latch);

        startStreamAndWaitOrExitOnFailure(streams, latch);

        System.exit(0);
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
