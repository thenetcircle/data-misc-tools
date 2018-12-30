package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.storage.jdbc.JdbcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class TncJdbcOutputFmt implements OutputFormat<NullWritable, MapWritable>,
    HiveOutputFormat<NullWritable, MapWritable> {

    private static final Logger log = LoggerFactory.getLogger(JdbcInputFormat.class);

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc,
                                            Path finalOutPath,
                                            Class<? extends Writable> valueClass,
                                            boolean isCompressed,
                                            Properties tableProperties,
                                            Progressable progress) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("jobConf: " + jc);
            log.debug("tableProperties: " + tableProperties);
        }

        return new TncJdbcRecordWriter(jc);
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable>
    getRecordWriter(FileSystem fs,
                    JobConf jobConf,
                    String s,
                    Progressable progressable) throws IOException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }

    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf jobConf) throws IOException {    }
}
