package org.ut.cs.dataeng_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
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

        //builder.stream("kym").to("kym-cleaned");

        KStream<String, String> kymStream = builder.stream("kym");
//        kymStream.to("kym-cleaned");
        //TODO more filtering: remove duplicates(title, url, last_update_source)
        //TODO title from url
        //TODO get origin and year from details

        //CLEANING
        KStream<String, String> filtered = kymStream.filter(new Cleaner());

        KStream<String, String> mapped = filtered.mapValues(new Converter());

        mapped.to("kym-cleaned");

        KStream<String, String> kymsStream = builder.stream("kyms");
        //CLEANING
        KStream<String, String> kymsFiltered = kymsStream.filter(new KymsCleaner());

        KStream<String, String> kymsMapped = kymsFiltered.mapValues(new KymsConverter());

        kymsMapped.to("kyms-cleaned");

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("starting");
            streams.start();
            System.out.println("started");
            latch.await();
        } catch (Throwable e) {
            System.out.println("failed." + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("exiting");
        System.exit(0);
    }

    static class Cleaner implements Predicate<String, String> {

        @Override
        public boolean test(String key, String value) {
            try {
                JSONObject jsonObject = new JSONObject(value);
                return Objects.equals(jsonObject.getString("category"), "Meme");
            } catch (Exception e) {
                LOG.warn("Failed to parse element", e);
            }
            return false;
        }
    }

    static class KymsCleaner implements Predicate<String, String> {

        @Override
        public boolean test(String key, String value) {
            LOG.info("parsing_key:" + key);
            LOG.info("parsing_value:" + value);
            try {
                JSONObject jsonObject = new JSONObject(value);
                return jsonObject.has("Resources");
            } catch (Exception e) {
                LOG.warn("Failed to parse element", e);
            }
            return false;
        }
    }

    static class KymsConverter implements ValueMapper<String, String> {

        @Override
        public String apply(String value) {
            JSONObject jsonObject = new JSONObject(value);
            JSONArray resourcesArr = jsonObject.optJSONArray("DBPedia_resources");
            if (resourcesArr != null) {
                jsonObject.put("DBPedia_resources_n", resourcesArr.length());
            }
            return jsonObject.toString();
        }
    }

    static class Converter implements ValueMapper<String, String> {

        private static List<String> fields = Arrays.asList("title", "url", "year_added", "meta", "details", "tags", "parent", "siblings", "children", "search_keywords");

        @Override
        public String apply(String value) {
            JSONObject jsonObject = new JSONObject(value);
            jsonObject.put("year_added", getParsedDate(jsonObject));
            jsonObject = project(jsonObject);
            jsonObject = aggregates(jsonObject);
            return jsonObject.toString();
        }

        private String getParsedDate(JSONObject jsonObject) {
            String formattedDate = "0001-01-01";
            try {
                long timeFromEpoch = jsonObject.optLong("added", -1);
                if (timeFromEpoch > 0) {
                    Instant date = Instant.ofEpochMilli(timeFromEpoch);
                    formattedDate = date.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE);
                    jsonObject.put("year_added", formattedDate);
                }
            } catch (Exception e) {
                LOG.warn("failed to parse date in added field", e);
            }
            return formattedDate;
        }

        private JSONObject aggregates(JSONObject jsonObject) {
            JSONObject result = jsonObject;
            JSONArray tagsArr = jsonObject.optJSONArray("tags");
            if (tagsArr != null) {
                result.put("tags_n", tagsArr.length());
            }
            JSONArray siblingsArr = jsonObject.optJSONArray("siblings");
            if (siblingsArr != null) {
                result.put("siblings_n", siblingsArr.length());
            }
            JSONArray childrenArr = jsonObject.optJSONArray("children");
            if (childrenArr != null) {
                result.put("children_n", childrenArr.length());
            }
            JSONArray descriptionArr = jsonObject.optJSONArray("description");
            if (descriptionArr != null) {
                result.put("description_n", descriptionArr.length());
            }
            return result;
        }

        private JSONObject project(JSONObject jsonObject) {
            JSONObject projected = new JSONObject();
            Map<String, Object> mapped = jsonObject.toMap();
            for (Map.Entry<String, Object> entry : mapped.entrySet()) {
                if (fields.contains(entry.getKey())) {
                    projected.put(entry.getKey(), entry.getValue());
                }
            }
            return projected;
        }
    }
}
