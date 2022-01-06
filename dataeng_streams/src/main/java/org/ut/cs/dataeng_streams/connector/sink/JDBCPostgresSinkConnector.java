package org.ut.cs.dataeng_streams.connector.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.ut.cs.dataeng_streams.util.PropertiesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCPostgresSinkConnector extends SinkConnector {

    private Map<String, String> originalProps;
    private JDBCConnectorConfig config;

    @Override
    public void start(Map<String, String> map) {
        this.config = new JDBCConnectorConfig(originalProps);
        this.originalProps = originalProps;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JDBCSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(new HashMap<>(originalProps));
        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return JDBCConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }
}
