package org.ut.cs.dataeng_streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.ut.cs.dataeng_streams.function.KymConverter;
import org.ut.cs.dataeng_streams.function.KymFilter;
import org.ut.cs.dataeng_streams.model.TopicName;

public class KymCleaner {

    private final StreamsBuilder builder;

    public KymCleaner(StreamsBuilder builder) {
        this.builder = builder;
    }

    public void clean() {
        convertKym(filterKym(buildKymStreamFromTopic())).to(TopicName.KYM_CLEANED.getValue());
    }

    private KStream<String, String> buildKymStreamFromTopic() {
        return builder.stream(TopicName.KYM.getValue());
    }

    private KStream<String, String> filterKym(KStream<String, String> stream) {
        return stream.filter(new KymFilter());
    }

    private KStream<String, String> convertKym(KStream<String, String> stream) {
        return stream.map(new KymConverter());
    }
}
