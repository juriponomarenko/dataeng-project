package org.ut.cs.dataeng_streams.model;

public enum TopicName {
    KYM,
    SPOTLIGHT,
    KYM_CLEANED,
    SPOTLIGHT_CLEANED,
    JOINED_STREAM,
    ;

    public String getValue(){
        return name().toLowerCase();
    }
}
