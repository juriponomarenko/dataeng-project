package org.ut.cs.dataeng_streams.connector.kym;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.util.PropertiesUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class HTTPSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSourceTask.class);

    private HTTPConnectorConfig config;
    private int monitorThreadTimeout;
    private BufferedReader bufferedReader;

    public HTTPSourceTask() {
    }

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new HTTPConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(HTTPConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG);
        try {

            URL url = new URL(config.getString(HTTPConnectorConfig.FILE_URL_PARAM_CONFIG));
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            InputStream inputStream = urlConnection.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(monitorThreadTimeout);
        List<SourceRecord> records = new ArrayList<>();

        try {
            String value;
            if ((value = bufferedReader.readLine()) != null) {
                if(value.length() > 1) {
                    records.add(new SourceRecord(
                            Collections.singletonMap("file", config.getString(HTTPConnectorConfig.FILE_URL_PARAM_CONFIG)),
                            Collections.singletonMap("offset", 0),
                            config.getString(HTTPConnectorConfig.KAFKA_TOPIC_CONFIG), null, null, null,
                            Schema.BYTES_SCHEMA,
                            value.getBytes()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    @Override
    public void stop() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
