package org.ut.cs.dataeng_streams.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
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

public class KymConverter implements KeyValueMapper<String, String, KeyValue<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(KymConverter.class);

    private static final String DEFAULT_DATE = "001-01-01";

    private static final List<String> FIELDS = Arrays.asList(JsonField.TITLE.getValue(),
            JsonField.URL.getValue(), JsonField.YEAR_ADDED.getValue(), JsonField.META.getValue(), JsonField.DETAILS.getValue(),
            JsonField.TAGS.getValue(), JsonField.PARENT.getValue(), JsonField.SIBLINGS.getValue(), JsonField.CHILDREN.getValue(),
            JsonField.SEARCH_KEYWORDS.getValue());

    @Override
    public KeyValue<String,String> apply(String key, String value) {
        //try {
            return applyConversions(key, value);
//        } catch (Exception e) {
//            LOG.error("Failed to convert: " + value, e);
//        }
//        return value;
    }

    private KeyValue<String,String> applyConversions(String key, String value) {
        JSONObject jsonObject = new JSONObject(value);
        jsonObject = addYear(jsonObject);
        jsonObject = project(jsonObject);
        jsonObject = putDescriptionFromMetaToRoot(jsonObject);
        jsonObject = putOriginFromDetailsToRoot(jsonObject);
        jsonObject = putYearFromDetailsToRoot(jsonObject);
        //jsonObject = overwriteTitleFromUrl(jsonObject);
        String titleKey = constructTitleFromUrl(jsonObject, jsonObject.getString(JsonField.TITLE.getValue()));
        jsonObject = aggregates(jsonObject);
        return new KeyValue<>(titleKey, jsonObject.toString());
    }

    private JSONObject addYear(JSONObject jsonObject) {
        jsonObject.put(JsonField.YEAR_ADDED.getValue(), getParsedDate(jsonObject));
        return jsonObject;
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
        jsonObject = countAndPutArrayElements(JsonField.TAGS, JsonField.TAGS_N, jsonObject);
        jsonObject = countAndPutArrayElements(JsonField.SIBLINGS, JsonField.SIBLINGS_N, jsonObject);
        jsonObject = countAndPutArrayElements(JsonField.CHILDREN, JsonField.CHILDREN_N, jsonObject);
        jsonObject = countAndPutArrayElements(JsonField.DESCRIPTION, JsonField.DESCRIPTION_N, jsonObject);

        return jsonObject;
    }

    private JSONObject countAndPutArrayElements(JsonField sourceFieldName, JsonField targetFieldName, JSONObject jsonObject) {
        JSONArray jsonArray = jsonObject.optJSONArray(sourceFieldName.getValue());
        if (jsonArray != null) {
            jsonObject.put(targetFieldName.getValue(), jsonArray.length());
        }
        return jsonObject;
    }

    private JSONObject project(JSONObject jsonObject) {
        JSONObject projected = new JSONObject();
        for (String key : FIELDS) {
            Object valueObject = jsonObject.opt(key);
            if (valueObject != null) {
                projected.put(key, valueObject);
            }
        }
        return projected;
    }

    private JSONObject putDescriptionFromMetaToRoot(JSONObject jsonObject) {
        JSONObject jsonMeta = jsonObject.getJSONObject(JsonField.META.getValue());
        jsonObject.put(JsonField.DESCRIPTION.getValue(), jsonMeta.getString(JsonField.DESCRIPTION.getValue()));
        return jsonObject;
    }

    private JSONObject putOriginFromDetailsToRoot(JSONObject jsonObject) {
        JSONObject jsonDetails = jsonObject.optJSONObject(JsonField.DETAILS.getValue());
        if (jsonDetails != null) {
            String origin = jsonDetails.optString(JsonField.ORIGIN.getValue());
            if (origin != null) {
                jsonObject.put(JsonField.ORIGIN.getValue(), origin.toLowerCase());
            } else {
                jsonObject.put(JsonField.ORIGIN.getValue(), JSONObject.NULL);
            }
        }
        return jsonObject;
    }

    private JSONObject putYearFromDetailsToRoot(JSONObject jsonObject) {
        JSONObject jsonDetails = jsonObject.optJSONObject(JsonField.DETAILS.getValue());
        if (jsonDetails != null) {
            jsonObject.put(JsonField.YEAR.getValue(), jsonDetails.optString(JsonField.YEAR.getValue(), DEFAULT_DATE));
        }
        return jsonObject;
    }

    private JSONObject overwriteTitleFromUrl(JSONObject jsonObject) {
        jsonObject.put(JsonField.TITLE.getValue(), constructTitleFromUrl(jsonObject, jsonObject.getString(JsonField.TITLE.getValue())));
        return jsonObject;
    }

    private String constructTitleFromUrl(JSONObject jsonObject, String defaultValue) {
        String result = defaultValue;
        String url = jsonObject.getString(JsonField.URL.getValue());
        String[] urlParts = StringUtils.splitByWholeSeparator(url, "memes/");
        if (urlParts.length > 1) {
            result = urlParts[1].replace("-", " ").toLowerCase();
        }
        return result;
    }
}
