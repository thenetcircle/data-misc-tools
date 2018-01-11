import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJava;


public class MiscTests {
    private static transient final Logger log = LoggerFactory.getLogger(MiscTests.class);

    @Test
    public void testUrl() throws UnsupportedEncodingException {
        String urlStr = "http://selma:8984/solr/poppenuser/select?fl=gender,age,nickname&q=city:'Bad Krozingen'&wt=json";

        log.info(urlStr);

        log.info(URLEncoder.encode(urlStr, "UTF-8"));
    }

    @Test
    public void testDateFmt() throws ParseException {
//        log.info(DateUtils.parseDate("2011-02-10T15:04:55Z", "yyyy-MM-dd'T'HH:mm:ss'Z'").toString());
        log.info(DateUtils.parseDate("2018-01-09T07:13:03+01:00", "yyyy-MM-dd'T'HH:mm:ss+HH:mm").toString());
        log.info(DateUtils.parseDate("2018-01-09T07:13:03", "yyyy-MM-dd'T'HH:mm:ss+HH:mm").toString());
    }

    @Test
    public void testParamType() throws Exception {
        System.out.println(getTypeEntryFromPrimitiveJava(String.class));
    }
}
