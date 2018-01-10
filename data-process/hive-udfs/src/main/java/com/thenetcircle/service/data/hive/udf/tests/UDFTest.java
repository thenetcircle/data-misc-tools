package com.thenetcircle.service.data.hive.udf.tests;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "udf_tests",
    value = "_FUNC_(obj) tells some info of the parameter")
public class UDFTest extends UDF {
    private static final Logger log = LoggerFactory.getLogger(UDFTest.class);

    public UDFTest() {
        System.out.printf("\n%s created\n%s\n\n", this, StringUtils.join(Thread.currentThread().getStackTrace(), "\n"));
    }

    public String evaluate(Object obj) {
        System.out.printf("\n%s is evaluating\n%s\n\n", this, StringUtils.join(Thread.currentThread().getStackTrace(), "\n"));
        return ToStringBuilder.reflectionToString(obj, ToStringStyle.MULTI_LINE_STYLE);
    }

    public void close() {
        System.out.printf("\n%s is closing\n%s\n\n", this, StringUtils.join(Thread.currentThread().getStackTrace(), "\n"));
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
    }
}
