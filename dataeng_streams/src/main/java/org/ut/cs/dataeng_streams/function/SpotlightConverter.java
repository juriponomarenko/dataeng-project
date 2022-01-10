package org.ut.cs.dataeng_streams.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.model.JsonField;

import java.math.BigDecimal;
import java.util.*;

public class SpotlightConverter implements KeyValueMapper<String, String, KeyValue<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SpotlightConverter.class);

    private static final BigDecimal THRESHOLD = new BigDecimal("0.99");

    private static final List<String> FIELDS = Arrays.asList(JsonField.TITLE.getValue(), JsonField.DBPEDIA_RESOURCES.getValue(),
            JsonField.DBPEDIA_RESOURCES_N.getValue());

    @Override
    public KeyValue<String,String> apply(String key, String value) {
//        LOG.info("key: " + key);
//        LOG.info("value: " + value);
        String title = constructTitleFromUrl(key);
        JSONObject jsonObject = new JSONObject(value);
        jsonObject = analyseResources(jsonObject);
        JSONArray resourcesArr = jsonObject.optJSONArray(JsonField.DBPEDIA_RESOURCES.getValue());
        if (resourcesArr != null) {
            jsonObject.put(JsonField.DBPEDIA_RESOURCES_N.getValue(), resourcesArr.length());
        }
        jsonObject = project(jsonObject);
        KeyValue<String,String> kv = new KeyValue<>(title, jsonObject.toString());
        return kv;
    }

    private JSONObject analyseResources(JSONObject jsonObject) {
        JSONArray resourcesArray = jsonObject.getJSONArray(JsonField.RESOURCES.getValue());
        Iterator<Object> iter = resourcesArray.iterator();
        Set<String> resources = new HashSet<>();
        while (iter.hasNext()) {
            Object elem = iter.next();
            if (elem instanceof JSONObject) {
                JSONObject resourceElem = (JSONObject) elem;
                if (resourceElem.optBigDecimal(JsonField.SIMILARITY_SCORE.getValue(), BigDecimal.ZERO).compareTo(THRESHOLD) == 1) {
                    String uri = resourceElem.optString(JsonField.URI.getValue());
                    if (uri != null) {
                        String[] uriParts = StringUtils.splitByWholeSeparator(uri, "resource/");
                        if (uriParts.length > 1) {
                            String ending = StringUtils.replaceChars(uriParts[1], "-_", "  ");
                            resources.add(ending);
                        }
                    }
                }
            }
        }
        jsonObject.put(JsonField.DBPEDIA_RESOURCES.getValue(), resources);
        return jsonObject;
    }

    private String constructTitleFromUrl(String url) {
        String result = url;
        //String url = jsonObject.getString(JsonField.URL.getValue());
        String[] urlParts = StringUtils.splitByWholeSeparator(url, "memes/");
        if (urlParts.length > 1) {
            result = urlParts[1].replace("-", " ").toLowerCase();
        }
        return result;
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
}
