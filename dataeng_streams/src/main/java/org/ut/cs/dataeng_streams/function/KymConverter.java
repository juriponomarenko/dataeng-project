package org.ut.cs.dataeng_streams.function;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.model.JsonField;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KymConverter implements ValueMapper<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(KymConverter.class);

    private static final String DEFAULT_DATE = "001-01-01";

    private static List<String> fields = Arrays.asList(JsonField.TITLE.getValue(),
            JsonField.URL.getValue(), JsonField.YEAR_ADDED.getValue(), JsonField.META.getValue(), JsonField.DETAILS.getValue(),
            JsonField.TAGS.getValue(), JsonField.PARENT.getValue(), JsonField.SIBLINGS.getValue(), JsonField.CHILDREN.getValue(),
            JsonField.SEARCH_KEYWORDS.getValue());

    @Override
    public String apply(String value) {
        JSONObject jsonObject = new JSONObject(value);
        jsonObject.put(JsonField.YEAR_ADDED.getValue(), getParsedDate(jsonObject));
        jsonObject = project(jsonObject);
        jsonObject = aggregates(jsonObject);
        return jsonObject.toString();
    }

    private String getParsedDate(JSONObject jsonObject) {
        String formattedDate = DEFAULT_DATE;
        try {
            long timeFromEpoch = jsonObject.optLong(JsonField.ADDED.getValue(), -1);
            if (timeFromEpoch > 0) {
                Instant date = Instant.ofEpochMilli(timeFromEpoch);
                formattedDate = date.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE);
                jsonObject.put(JsonField.YEAR_ADDED.getValue(), formattedDate);
            }
        } catch (Exception e) {
            LOG.warn("failed to parse date in added field", e);
        }
        return formattedDate;
    }

    private JSONObject aggregates(JSONObject jsonObject) {
        JSONObject result = jsonObject;
        result = countAndPutArrayElements(JsonField.TAGS, JsonField.TAGS_N, result);
        result = countAndPutArrayElements(JsonField.SIBLINGS, JsonField.SIBLINGS_N, result);
        result = countAndPutArrayElements(JsonField.CHILDREN, JsonField.CHILDREN_N, result);
        result = countAndPutArrayElements(JsonField.DESCRIPTION, JsonField.DESCRIPTION_N, result);

        return result;
    }

    private JSONObject countAndPutArrayElements(JsonField sourceFieldName, JsonField targetFieldName, JSONObject jsonObject) {
        JSONObject result = jsonObject;
        JSONArray jsonArray = jsonObject.optJSONArray(sourceFieldName.getValue());
        if (jsonArray != null) {
            result.put(targetFieldName.getValue(), jsonArray.length());
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

    private void buildDescriptionFromMeta(JSONObject jsonObject){
        JSONArray resultingArray = new JSONArray();
        JSONArray jsonArray = jsonObject.optJSONArray(JsonField.META.getValue());
        if(jsonArray != null){
            resultingArray.put()
        }
    }
}
