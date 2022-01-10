package org.ut.cs.dataeng_streams.function;

import org.apache.kafka.streams.kstream.Predicate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.model.JsonField;

public class SpotlightFilter implements Predicate<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SpotlightFilter.class);

    @Override
    public boolean test(String key, String value) {
        try {
            JSONObject jsonObject = new JSONObject(value);
            return jsonObject.has(JsonField.RESOURCES.getValue());
        } catch (Exception e) {
            LOG.warn("Failed to parse element", e);
        }
        return false;
    }
}
