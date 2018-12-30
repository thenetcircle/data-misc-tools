package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.storage.jdbc.JdbcStorageHandler;

public class TncJdbcStroageHandler extends JdbcStorageHandler {
    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return TncJdbcSerDe.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return TncJdbcOutputFmt.class;
    }
}
