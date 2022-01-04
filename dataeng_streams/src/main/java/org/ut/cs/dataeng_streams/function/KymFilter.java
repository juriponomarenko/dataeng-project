package org.ut.cs.dataeng_streams.function;

import org.apache.kafka.streams.kstream.Predicate;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.model.JsonField;

import java.util.Objects;

public class KymFilter implements Predicate<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(KymFilter.class);

    private static final String MEME_CATEGORY_VALUE = "Meme";

    @Override
    public boolean test(String key, String value) {
        try {
            JSONObject jsonObject = new JSONObject(value);
            return Objects.equals(jsonObject.optString(JsonField.CATEGORY.getValue()), MEME_CATEGORY_VALUE);
        } catch (Exception e) {
            LOG.warn("Failed to parse element", e);
        }
        return false;
    }
}
