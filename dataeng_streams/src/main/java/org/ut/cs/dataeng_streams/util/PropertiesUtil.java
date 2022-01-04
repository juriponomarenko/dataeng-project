package org.ut.cs.dataeng_streams.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public final class PropertiesUtil {

    private static final String CONNECTOR_VERSION = "connector.version";

    private static Logger log = LoggerFactory.getLogger(PropertiesUtil.class);
    private static String propertiesFile = "/kafka-connect-dataeng.properties";
    private static Properties properties;

    static {
        try (InputStream stream = PropertiesUtil.class.getResourceAsStream(propertiesFile)) {
            properties = new Properties();
            properties.load(stream);
        } catch (Exception ex) {
            log.warn("Error while loading properties: ", ex);
        }
    }

    public static String getConnectorVersion() {
        return properties.getProperty(CONNECTOR_VERSION);
    }

    private PropertiesUtil() {
    }

}
