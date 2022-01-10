package org.ut.cs.dataeng_streams.function;

import org.apache.kafka.streams.kstream.Reducer;

public class KymReducer implements Reducer<String> {
    @Override
    public String apply(String value1, String value2) {
        return value2;
    }
}
