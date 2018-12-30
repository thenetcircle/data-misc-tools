package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import java.io.Serializable;

public class JdbcColumnInfo implements Serializable {
    public String name;
    public int type;
    public int index;
    public int length;
    public boolean isNullable;
    public boolean isAutoIncrement;
}
