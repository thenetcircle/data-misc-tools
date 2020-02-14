package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.thenetcircle.service.data.hive.jdbc.storagehandler.ITncJdbcHelper.*;
import static java.lang.String.format;

public class TncJdbcRecordWriter implements RecordWriter {
    private static final Log log = LogFactory.getLog(TncJdbcRecordWriter.class);

    protected JobConf conf = null;

    protected DataSource dbcpDataSource = null;
    protected PreparedStatement ps;
    protected Connection conn;

    protected int counter = 0;
    protected int totalCount = 0;
    protected String tableName;
    protected JdbcColumnInfo[] columnInfos;
    protected Map<String, Text> nameToText = new HashMap<>();
    protected int fetchSize = 1000;


    public TncJdbcRecordWriter(JobConf jc) {
        this.conf = jc;
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace(SessionState.get().err);
        }
    }

    protected void init() throws IOException {
        if (dbcpDataSource == null) {
            try {
                dbcpDataSource = BasicDataSourceFactory.createDataSource(getConnPoolProps(conf));
            } catch (Exception e) {
                log.error("failed to create datasource", e);
                e.printStackTrace(SessionState.get().err);
            }
        }

        try {
            if (conn == null || conn.isClosed() || conn.isReadOnly()) {
                if (conn != null) conn.close();
                conn = dbcpDataSource.getConnection();
                conn.setAutoCommit(false);
            }
        } catch (Exception e) {
            log.error("failed to create connection", e);
        }

        if (StringUtils.isEmpty(tableName)) {
            tableName = getTableName(this.conf);
            if (StringUtils.isBlank(tableName)) {
                throw new IllegalArgumentException("hive.sql.table is not defined");
            }
            try {
                columnInfos = getColInfos(conf, conn);
                Stream.of(columnInfos).map(ci -> ci.name).forEach(n -> nameToText.put(n, new Text(n)));
            } catch (HiveJdbcDatabaseAccessException e) {
                throw new IOException(e);
            } catch (Exception e) {
                log.error("failed to create connection", e);
                e.printStackTrace(SessionState.get().err);
            }
        }

        fetchSize = getFetchSize(conf);
    }

    @Override
    public void write(Writable w) throws IOException {
        initInsertStatement();
        MapWritable mw = (MapWritable) w;
        try {
            for (JdbcColumnInfo jci : columnInfos) {
                Writable wv = mw.get(nameToText.get(jci.name));//ugly
                Object val = HiveJdbcBridgeUtils.getValue(wv);
                ps.setObject(jci.index, val);
            }
            ps.addBatch();
            totalCount++;
            counter++;

            if (counter == fetchSize) {
                counter = 0;
                sendBatch();
            }
        } catch (SQLException e) {
            log.error(format("failed to set \n\t %s ", w));
            throw new IOException(e);
        }
    }

    protected void sendBatch() throws SQLException {
        int[] reCnts = ps.executeBatch();
        log.info(format("inserted %d into table %s in batch", ps.getUpdateCount(), tableName));
        conn.commit();
    }

    protected void initInsertStatement() throws IOException {
        String sql = "unprepared";
        try {
            if (ps != null && !ps.isClosed()) return;
            String[] columnNames = Stream.of(columnInfos).map(ci -> ci.name).toArray(String[]::new);
            sql = insertRowSql(tableName, columnNames);
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            log.error("failed to prepare statement with sql:\n\t" + sql, e);
            e.printStackTrace(SessionState.get().err);
            throw new IOException(e);
        }
    }

    @Override
    public void close(boolean abort) throws IOException {
        try (Connection conn = this.conn) {
            if (counter > 0)
                sendBatch();
            counter = 0;
            if (abort)
                conn.rollback();
        } catch (SQLException e) {
            log.error("failed to close connection", e);
            e.printStackTrace(SessionState.get().err);
            throw new IOException(e);
        } finally {
            counter = 0;
        }
    }
}
