package org.ut.cs.dataeng_streams.connector.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.connector.kym.HTTPConnectorConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSinkTask.class);

    //Object a = DriverManager.getDriver("").getConnection();

    private JDBCConnectorConfig config;

    private Connection connection;

    private final AtomicLong idProvider = new AtomicLong(1);

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new JDBCConnectorConfig(properties);
        try {
            connection = DriverManager.getConnection(config.getString(JDBCConnectorConfig.CONNECTION_URL_PARAM_CONFIG), "postgres", "postgres");
        } catch (Exception e){
            LOG.error("Failed to connect", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for(SinkRecord record : collection){
            try {
                String title = record.key().toString();
                Statement s = connection.createStatement();
                int affected = s.executeUpdate("insert into meme_fact(id, title, url) values(" + idProvider.incrementAndGet() + ", " + title + ", 'abuu')");
                LOG.info("Inserted " + affected);
            } catch (Exception e){
                LOG.error("Failed to execute statement", e);
            }
        }
    }

    @Override
    public void stop() {
        if(connection != null){
            try {
                connection.close();
            } catch(Exception e){
                LOG.warn("Failed to close connection", e);
            }
        }
    }
}
