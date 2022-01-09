package org.ut.cs.dataeng_streams.connector.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JDBCConnectorConfig extends AbstractConfig {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnectorConfig.class);

    public JDBCConnectorConfig(final Map<?, ?> originalProps) {
        super(CONFIG_DEF, pr(originalProps));
    }

    public static final String KAFKA_TOPIC_CONFIG = "topics";
    private static final String KAFKA_TOPIC_DOC = "This is the topics to read from.";

    public static final String CONNECTION_URL_PARAM_CONFIG = "connection.url";
    private static final String CONNECTION_URL_PARAM_DOC = "This is the defined url to use for connecting to db.";

    public static final String MONITOR_THREAD_TIMEOUT_CONFIG = "monitor.thread.timeout";
    private static final String MONITOR_THREAD_TIMEOUT_DOC = "Timeout used by the monitoring thread";
    private static final int MONITOR_THREAD_TIMEOUT_DEFAULT = 10000;

    private static Map<?, ?> pr(Map<?, ?> originalProps){
        LOG.info("originalProps: " + originalProps.toString());
        return originalProps;
    }

    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addParams(configDef);
        return configDef;
    }

    private static void addParams(final ConfigDef configDef) {
        configDef
                .define(
                        CONNECTION_URL_PARAM_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONNECTION_URL_PARAM_DOC)

                .define(
                        KAFKA_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        KAFKA_TOPIC_DOC)
                .define(
                        MONITOR_THREAD_TIMEOUT_CONFIG,
                        ConfigDef.Type.INT,
                        MONITOR_THREAD_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        MONITOR_THREAD_TIMEOUT_DOC);
    }

    public static void main(String[] args) {
        Map<String,String> m = new HashMap<>();
        JDBCConnectorConfig c = new JDBCConnectorConfig(m);
    }
}
