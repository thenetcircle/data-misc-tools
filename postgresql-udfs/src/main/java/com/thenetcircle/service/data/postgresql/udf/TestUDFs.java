package com.thenetcircle.service.data.postgresql.udf;

import org.postgresql.pljava.PooledObject;
import org.postgresql.pljava.ResultSetProvider;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;

public class TestUDFs implements ResultSetProvider, PooledObject {
    private static Logger log = Logger.getAnonymousLogger();

    public static AtomicInteger C = new AtomicInteger(0);

    private Object param;

    public TestUDFs(Object _param) {
        this.param = _param;
    }

    public static ResultSetProvider test(Object param) {
        return new TestUDFs(param);
    }

    @Override
    public void activate() throws SQLException {
        log.info(format("%d, activate", C.getAndIncrement()));
    }

    @Override
    public void passivate() throws SQLException {
        log.info(format("%d, passivate", C.getAndIncrement()));
    }

    @Override
    public void remove() {
        log.info(format("%d, remove", C.getAndIncrement()));
    }

    @Override
    public boolean assignRowValues(ResultSet rs, int i) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        StringBuilder sb = new StringBuilder();

        for (int mi = 1, j = md.getColumnCount(); mi < j; mi++) {
            sb.append(format("%s: %s, ", md.getColumnName(mi), md.getColumnTypeName(mi)));
        }
        log.info(sb.toString());

        rs.updateInt(1, i);
        rs.updateString(2, param.getClass().isArray() ? Arrays.toString((Object[]) param) : String.valueOf(param));

        return false;
    }

    @Override
    public void close() throws SQLException {
        log.info(format("%d, close", C.getAndIncrement()));
    }
}
