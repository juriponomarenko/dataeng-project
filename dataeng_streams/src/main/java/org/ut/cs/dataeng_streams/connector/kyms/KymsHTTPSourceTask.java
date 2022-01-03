package org.ut.cs.dataeng_streams.connector.kyms;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.PropertiesUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


public class KymsHTTPSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(KymsHTTPSourceTask.class);

    private KymsHTTPConnectorConfig config;
    private int monitorThreadTimeout;
    private BufferedReader bufferedReader;
    private volatile int counter = 0;
    private final int limit = 2;
    private volatile long mingiCounter = 0;

    public KymsHTTPSourceTask() {
    }

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new KymsHTTPConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(KymsHTTPConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG);
        try {

            URL url = new URL(config.getString(KymsHTTPConnectorConfig.FILE_URL_PARAM_CONFIG));
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            InputStream inputStream = urlConnection.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            //csvReader.readLine();
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

            if ((value = bufferedReader.readLine()) != null && counter < limit) {
                if(value.length() > 1) {
                    //counter++;
                    mingiCounter++;
                    String[] elems = StringUtils.split(value, "\"", 2);
                    if(elems.length == 2){
                        String key = elems[0];
                        key = StringUtils.strip(key, "\" :");
                        String valuePart = elems[1];
                        valuePart = StringUtils.strip(valuePart, "\" :");
                        records.add(new SourceRecord(
                                Collections.singletonMap("file", config.getString(KymsHTTPConnectorConfig.FILE_URL_PARAM_CONFIG)),
                                Collections.singletonMap("offset", 0),
                                config.getString(KymsHTTPConnectorConfig.KAFKA_TOPIC_CONFIG), null, null, key.getBytes(),
                                Schema.BYTES_SCHEMA,
                                valuePart.getBytes()));
                    }

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
