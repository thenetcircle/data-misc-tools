package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.*;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.thenetcircle.service.data.hive.jdbc.storagehandler.ITncJdbcHelper.getColInfos;
import static com.thenetcircle.service.data.hive.jdbc.storagehandler.ITncJdbcHelper.getDataSource;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;

public class TncJdbcSerDe extends AbstractSerDe {
    private static final Logger log = LoggerFactory.getLogger(TncJdbcSerDe.class);

    private JdbcColumnInfo[] columnInfos;
    private StructObjectInspector objectInspector;
    private List<String> columnNames;
    private int numColumns;
    private String[] hiveColumnTypeArray;
    private List<Object> row;
//    private DbRecWritable cachedWritable;

    @Override
    public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
        log.trace("Initializing the SerDe");
        if (!tbl.containsKey(JdbcStorageConfig.DATABASE_TYPE.getPropertyName())) {
            log.error("there is not database.type!");
            return;
        }

        Configuration tableConfig = null;
        try {
            tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(tbl);

            DataSource ds = getDataSource(tableConfig);
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                this.columnInfos = getColInfos(tableConfig, conn);
            }
            columnNames = Stream.of(columnInfos).map(ci -> ci.name).collect(Collectors.toList());
            numColumns = columnInfos.length;

            List<String> hiveColumnNames;
            String[] hiveColumnNameArray = StringUtils.split(tbl.getProperty(serdeConstants.LIST_COLUMNS), ',');
            if (numColumns != hiveColumnNameArray.length) {
                throw new SerDeException("Expected " + numColumns + " columns. Table definition has "
                    + hiveColumnNameArray.length + " columns");
            }
            hiveColumnNames = Arrays.asList(hiveColumnNameArray);

            hiveColumnTypeArray = StringUtils.split(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES), ':');
            List<ObjectInspector> fieldInspectors = new ArrayList<>(numColumns);

//            this.cachedWritable = new DbRecWritable(hiveTypesToSqlTypes(hiveColumnTypeArray));

            for (int i = 0; i < numColumns; i++) {
                PrimitiveTypeInfo ti = getPrimitiveTypeInfo(hiveColumnTypeArray[i]);
//                ObjectInspector oi = getPrimitiveWritableObjectInspector(ti);
                ObjectInspector oi = getPrimitiveJavaObjectInspector(ti);
                fieldInspectors.add(oi);
            }
            objectInspector = getStandardStructObjectInspector(hiveColumnNames, fieldInspectors);

            row = new ArrayList<>(numColumns);
        } catch (Exception e) {
            log.error("Caught exception while initializing the SqlSerDe", e);
            throw new SerDeException(e);
        }
    }

    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public MapWritable serialize(Object row, ObjectInspector insp) throws SerDeException {
        final StructObjectInspector structInsp = (StructObjectInspector) insp;
        final List<? extends StructField> fieldList = structInsp.getAllStructFieldRefs();
        if (fieldList.size() != numColumns) {
            throw new SerDeException(String.format(
                "Required %d columns, received %d.", numColumns,
                fieldList.size()));
        }

        Object[] fields = null;
        if (row.getClass().isArray()) {
            fields = (Object[]) row;
        } else {
            fields = new Object[]{row};
        }

// when it is direct insert using liberals, it is raw values in an array
        // when it is inserted with selected values, it is an array of writables
//        cachedWritable.clear();
        MapWritable mw = new MapWritable();
        for (int i = 0; i < numColumns; i++) {
            StructField sf = fieldList.get(i);
            if (sf == null) continue;

            Text key = new Text(columnInfos[i].name);
            Object field = structInsp.getStructFieldData(fields, sf);

            ObjectInspector fieldOI = sf.getFieldObjectInspector();
            mw.put(key, HiveJdbcBridgeUtils.deparseObject(field, fieldOI));
        }

        return mw;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        log.trace("Deserialize from SerDe");
        if (!(blob instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
        }

        if ((row == null) || (columnNames == null)) {
            throw new SerDeException("JDBC SerDe hasn't been initialized properly");
        }
        row.clear();

        MapWritable input = (MapWritable) blob;
        Text columnKey = new Text();
        for (int i = 0; i < numColumns; i++) {
            columnKey.set(columnNames.get(i));
            Writable value = input.get(columnKey);
            row.add(value instanceof NullWritable ? null : ((ObjectWritable) value).get());
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

}
