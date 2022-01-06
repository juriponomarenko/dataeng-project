package org.ut.cs.dataeng_streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.ut.cs.dataeng_streams.function.SpotlightConverter;
import org.ut.cs.dataeng_streams.function.SpotlightFilter;
import org.ut.cs.dataeng_streams.model.TopicName;

public class SpotlightCleaner {

    private final StreamsBuilder builder;

    public SpotlightCleaner(StreamsBuilder builder) {
        this.builder = builder;
    }

    public void clean() {
        convertSpotlight(filterSpotlight(buildSpotlightStreamFromTopic())).to(TopicName.SPOTLIGHT_CLEANED.getValue());
    }

    private KStream<String, String> buildSpotlightStreamFromTopic() {
        return builder.stream(TopicName.SPOTLIGHT.getValue());
    }

    private KStream<String, String> filterSpotlight(KStream<String, String> stream) {
        return stream.filter(new SpotlightFilter());
    }

    private KStream<String, String> convertSpotlight(KStream<String, String> stream) {
        return stream.map(new SpotlightConverter());
    }
}
