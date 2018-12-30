package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.GenericJdbcDatabaseAccessor;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static org.apache.hive.storage.jdbc.conf.JdbcStorageConfig.*;

public interface ITncJdbcHelper {
    Logger log = LoggerFactory.getLogger(ITncJdbcHelper.class);
    String DBCP_CONFIG_PREFIX = JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
    Text DBCP_PWD = new Text(DBCP_CONFIG_PREFIX + ".password");
    int DEFAULT_FETCH_SIZE = 1000;

    static DataSource getDataSource(Configuration conf) throws Exception {
        try {
            return BasicDataSourceFactory.createDataSource(getConnPoolProps(conf));
        } catch (Exception e) {
            log.error("failed to create datasource", e);
            throw e;
        }
    }

    static Properties getDefaultDBCPProps() {
        Properties props = new Properties();
        props.put("initialSize", "1");
        props.put("maxActive", "3");
        props.put("maxIdle", "0");
        props.put("maxWait", "10000");
        props.put("timeBetweenEvictionRunsMillis", "30000");
        return props;
    }

    static Properties getConnPoolProps(Configuration conf) throws Exception {
        // Create the default properties object
        Properties dbProps = getDefaultDBCPProps();

        // override with user defined properties
        Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
        if ((userProperties != null) && (!userProperties.isEmpty())) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                dbProps.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
            }
        }

        // handle password
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        if (credentials.getSecretKey(DBCP_PWD) != null) {
            log.info("found token in credentials");
            dbProps.put(DBCP_PWD, new String(credentials.getSecretKey(DBCP_PWD)));
        }

        // essential properties that shouldn't be overridden by users
        dbProps.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
        dbProps.put("driverClassName", conf.get(JDBC_DRIVER_CLASS.getPropertyName()));
        dbProps.put("type", "javax.sql.DataSource");
        return dbProps;
    }

    static int getFetchSize(Configuration conf) {
        return conf.getInt(JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
    }

    static String getTableName(Configuration conf) {
        return conf.get(JdbcStorageConfig.TABLE.getPropertyName());
    }

    static String insertRowSql(String table, String[] fieldNames) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("Field names may not be null");
        }
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ").append(table);

        if (ArrayUtils.isNotEmpty(fieldNames)) {
            query.append(" (").append(StringUtils.join(fieldNames, ',')).append(")");
        }

        query.append(" VALUES (")
            .append(StringUtils.repeat("?", ",", fieldNames.length))
            .append(");");
        return query.toString();
    }

    static String getMetaDataQuery(Configuration conf) {
        String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
        String metadataQuery = addLimitToQuery(getDbType(conf), sql, 1);
        return metadataQuery;
    }

    static DatabaseType getDbType(Configuration conf) {
        return DatabaseType.valueOf(conf.get(DATABASE_TYPE.getPropertyName()));
    }

    DataSource getDataSource();

    GenericJdbcDatabaseAccessor getDbAccessor();

    static JdbcColumnInfo[] getColInfos(Configuration conf, Connection conn) throws HiveJdbcDatabaseAccessException {
        String metadataQuery = getMetaDataQuery(conf);
        try (PreparedStatement ps = conn.prepareStatement(metadataQuery);
             ResultSet rs = ps.executeQuery()) {

            ResultSetMetaData md = rs.getMetaData();
            int numColumns = md.getColumnCount();
            JdbcColumnInfo[] columnInfos = new JdbcColumnInfo[numColumns];
            for (int i = 0; i < numColumns; i++) {
                JdbcColumnInfo jci = new JdbcColumnInfo();
                int index = i + 1;
                jci.index = index;
                jci.type = md.getColumnType(index);
                jci.length = md.getScale(index);
                jci.name = md.getColumnName(index);
                jci.isNullable = md.isNullable(index) == ResultSetMetaData.columnNoNulls;
                jci.isAutoIncrement = md.isAutoIncrement(index);

                columnInfos[i] = jci;
            }
            return columnInfos;
        } catch (Exception e) {
            log.error("Error while trying to get column names.", e);
            throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
        }
    }

    static String addLimitToQuery(DatabaseType dbt, String sql, int limit) {
        if (StringUtils.isBlank(sql) || dbt == null || limit < 1) return sql;
        switch (dbt) {
            case POSTGRES:
            case MYSQL:
                return format("%s LIMIT %d", sql, limit);
            default:
                return format("%s {LIMIT %d}", sql, limit);
        }
    }

    static String addLimitAndOffsetToQuery(DatabaseType dbt, String sql, int limit, int offset) {
        if (StringUtils.isBlank(sql) || dbt == null || limit < 1) return sql;
        switch (dbt) {
            case POSTGRES:
            case MYSQL:
                return format("%s LIMIT %d OFFSET %d", sql, limit, offset);
            default:
                return format("%s {LIMIT %d OFFSET %d}", sql, limit, offset);
        }
    }
}
