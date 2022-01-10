package org.ut.cs.dataeng_streams.connector.vision;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static Logger LOG = LoggerFactory.getLogger(HTTPSourceTask.class);

    private HTTPConnectorConfig config;
    private int monitorThreadTimeout;
    private InputStream inputStream;

    public HTTPSourceTask() {
    }

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        //TODO streaming JSON parser
        config = new HTTPConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(HTTPConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG);
        try {

            URL url = new URL(config.getString(HTTPConnectorConfig.FILE_URL_PARAM_CONFIG));
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            inputStream = urlConnection.getInputStream();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(monitorThreadTimeout);
        List<SourceRecord> records = new ArrayList<>();

//        try {
//            //TODO
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return records;
    }

    @Override
    public void stop() {
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
