package org.ut.cs.dataeng_streams.function;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.ut.cs.dataeng_streams.model.JsonField;

public class SpotlightConverter implements ValueMapper<String, String> {

    @Override
    public String apply(String value) {
        JSONObject jsonObject = new JSONObject(value);
        JSONArray resourcesArr = jsonObject.optJSONArray(JsonField.DBPEDIA_RESOURCES.getValue());
        if (resourcesArr != null) {
            jsonObject.put(JsonField.DBPEDIA_RESOURCES_N.getValue(), resourcesArr.length());
        }
        return jsonObject.toString();
    }
}
