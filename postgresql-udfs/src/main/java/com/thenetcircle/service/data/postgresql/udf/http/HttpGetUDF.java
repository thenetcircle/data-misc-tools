package com.thenetcircle.service.data.postgresql.udf.http;

import org.postgresql.pljava.ResultSetProvider;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HttpGetUDF implements ResultSetProvider {

    @Override
    public boolean assignRowValues(ResultSet resultSet, int i) throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {

    }
}
