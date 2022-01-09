package org.ut.cs.dataeng_streams.connector.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.cs.dataeng_streams.connector.kym.HTTPConnectorConfig;
import org.ut.cs.dataeng_streams.function.KymConverter;
import org.ut.cs.dataeng_streams.model.JsonField;
import org.ut.cs.dataeng_streams.util.PropertiesUtil;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSinkTask.class);

    //Object a = DriverManager.getDriver("").getConnection();

    private JDBCConnectorConfig config;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private PreparedStatement preparedDateStatement;
    private PreparedStatement preparedOriginStatement;

    private final AtomicLong idProvider = new AtomicLong(1);

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        LOG.info("starting sink task");
        config = new JDBCConnectorConfig(properties);
        try {
            connection = DriverManager.getConnection(config.getString(JDBCConnectorConfig.CONNECTION_URL_PARAM_CONFIG), "postgres", "postgres");
            preparedStatement = connection.prepareStatement("insert into meme_fact(title, url, description, children_count, tags_count, keywords_count, adult, " +
                    "spoof, medical, racy, date_id, parent_id, origin_id, keyword_id, child_id, tag_id) VALUES " +
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    "(select id from date_dim where date = ?), " +
                    "?, " +
                    "(select id from origin_dim where origin = ?), " +
                    "?, ?, ?)");
            preparedDateStatement = connection.prepareStatement("insert into date_dim(date, month_number, month_name, year) values (?, ?, ?, ?) on conflict on constraint date_dim_date_key do nothing");
            preparedOriginStatement = connection.prepareStatement("insert into origin_dim(origin) values (?) on conflict on constraint origin_dim_origin_key do nothing");

            LOG.info("connection created");
        } catch (Exception e){
            LOG.error("Failed to connect", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        LOG.info("put records to db: " + collection.size());
        for(SinkRecord record : collection){
            LOG.info("inserting: " + record);
            try {
                String title = new String((byte[])record.key());
                LOG.info("parsed key in sink: " + title);
                String value = new String((byte[])record.value());
                JSONObject parsedJson = new JSONObject(value);
                JSONObject kymObject = parsedJson.getJSONObject("kym");
                String url =kymObject.getString(JsonField.URL.getValue());
                String description = kymObject.optString(JsonField.DESCRIPTION.getValue());
                int childrenCount = kymObject.optInt(JsonField.CHILDREN_N.getValue(), 0);
                int tagsCount = kymObject.optInt(JsonField.TAGS_N.getValue(), 0);
                int keywordsCount = 0; //TODO kymObject.optInt(JsonField.KEY)
                String adult = null; //TODO
                String spoof = null;
                String medical = null;
                String racy = null;

                String dateRaw = kymObject.optString(JsonField.YEAR_ADDED.getValue(), KymConverter.DEFAULT_DATE);
                LocalDate dateParsed = LocalDate.parse(dateRaw, DateTimeFormatter.ISO_LOCAL_DATE);
                insertDate(dateParsed);

                String origin = kymObject.optString(JsonField.ORIGIN.getValue());
                if(origin != null){
                    insertOrigin(origin);
                }


                int paramCounter = 1;
                preparedStatement.setString(paramCounter++, title);
                preparedStatement.setString(paramCounter++, url);
                setStringOrNull(preparedStatement, paramCounter++, description);
                setIntOrNull(preparedStatement, paramCounter++, childrenCount);
                setIntOrNull(preparedStatement, paramCounter++, tagsCount);
                setIntOrNull(preparedStatement, paramCounter++, keywordsCount);
                setStringOrNull(preparedStatement, paramCounter++, adult);
                setStringOrNull(preparedStatement, paramCounter++, spoof);
                setStringOrNull(preparedStatement, paramCounter++, medical);
                setStringOrNull(preparedStatement, paramCounter++, racy);

                //set FK-s
                //preparedStatement.setNull(paramCounter++, Types.BIGINT);
                preparedStatement.setDate(paramCounter++, Date.valueOf(dateParsed));
                preparedStatement.setNull(paramCounter++, Types.BIGINT);
                //preparedStatement.setNull(paramCounter++, Types.BIGINT);
                setStringOrNull(preparedStatement, paramCounter++, origin);
                //preparedStatement.setString(paramCounter++, origin);
                preparedStatement.setNull(paramCounter++, Types.BIGINT);
                preparedStatement.setNull(paramCounter++, Types.BIGINT);
                preparedStatement.setNull(paramCounter++, Types.BIGINT);

                int affected = preparedStatement.executeUpdate();
                //Statement s = connection.createStatement();
                //int affected = s.executeUpdate("insert into meme_fact(id, title, url, description, ) values(" + idProvider.incrementAndGet() + ", '" + title + "', 'abuu')");
                LOG.info("Inserted " + affected);
            } catch (Exception e){
                LOG.error("Failed to execute statement", e);
            }
        }
    }

    private void insertDate(LocalDate date) throws SQLException {
        preparedDateStatement.setDate(1, Date.valueOf(date));
        preparedDateStatement.setInt(2, date.getMonthValue());
        preparedDateStatement.setString(3, date.getMonth().name());
        preparedDateStatement.setInt(4, date.getYear());
        int affected = preparedDateStatement.executeUpdate();
        LOG.info("Inserted date_dim: " + affected);
    }

    private void insertOrigin(String origin) throws SQLException {
        preparedOriginStatement.setString(1, origin);
        int affected = preparedOriginStatement.executeUpdate();
        LOG.info("origin inserted: " + affected);
    }

    private void setStringOrNull(PreparedStatement preparedStatement, int parameterIndex, String value) throws SQLException {
        if(value != null){
            preparedStatement.setString(parameterIndex, value);
        } else {
            preparedStatement.setNull(parameterIndex, Types.VARCHAR);
        }
    }

    private void setIntOrNull(PreparedStatement preparedStatement, int parameterIndex, Integer value) throws SQLException {
        if(value != null){
            preparedStatement.setInt(parameterIndex, value);
        } else {
            preparedStatement.setNull(parameterIndex, Types.BIGINT);
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
