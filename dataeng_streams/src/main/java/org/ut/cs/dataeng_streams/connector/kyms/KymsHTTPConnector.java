package org.ut.cs.dataeng_streams.connector.kyms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.PropertiesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.ut.cs.dataeng_streams.connector.kym.KymHTTPConnectorConfig.CONFIG_DEF;

public class KymsHTTPConnector extends SourceConnector {

    private final Logger log = LoggerFactory.getLogger(KymsHTTPConnector.class);

    private Map<String, String> originalProps;
    private KymsHTTPConnectorConfig config;


    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KymsHTTPSourceTask.class;
    }

    @Override
    public void start(Map<String, String> originalProps) {
        this.config = new KymsHTTPConnectorConfig(originalProps);
        this.originalProps = originalProps;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(new HashMap<>(originalProps));
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

}
