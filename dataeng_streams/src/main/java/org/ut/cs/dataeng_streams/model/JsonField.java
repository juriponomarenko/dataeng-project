package org.ut.cs.dataeng_streams.model;

public enum JsonField {
    TITLE,
    URL,
    YEAR_ADDED,
    META,
    DETAILS,
    TAGS,
    TAGS_N,
    PARENT,
    SIBLINGS,
    SIBLINGS_N,
    CHILDREN,
    CHILDREN_N,
    SEARCH_KEYWORDS,
    CATEGORY,
    RESOURCES("Resources"),
    DBPEDIA_RESOURCES("DBPedia_resources"),
    DBPEDIA_RESOURCES_N("DBPedia_resources_n"),
    ADDED,
    DESCRIPTION,
    DESCRIPTION_N,
    ORIGIN,
    YEAR,
    ;

    private String value;

    JsonField() {
    }

    JsonField(String value) {
        this.value = value;
    }

    public String getValue() {
        if (value != null) {
            return this.value;
        }
        return this.name().toLowerCase();
    }

}
