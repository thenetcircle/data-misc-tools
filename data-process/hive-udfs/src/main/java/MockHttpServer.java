import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

public class MockHttpServer {
    static AtomicInteger counter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        InetSocketAddress addr = new InetSocketAddress(8000);
        System.out.printf("start http server at %s\n", addr);
        HttpServer server = HttpServer.create(addr, 10);
        server.createContext("/", (HttpExchange he) -> {
            he.getResponseHeaders().set(CONTENT_TYPE, JSON_UTF_8.toString());
            OutputStream respOS = he.getResponseBody();
            String respStr = String.format("{c:%d,now:'%s'}", counter.incrementAndGet(), DateFormatUtils.format(new Date(), "yyyy-MM-dd hh:mm:ss"));
            System.out.printf("receive request from %swith headers %s\n\treturning %s\n", he.getRemoteAddress(), StringUtils.join(he.getRequestHeaders().entrySet()), respStr);
            byte[] bytes = respStr.getBytes();
            he.sendResponseHeaders(200, bytes.length);
            respOS.write(bytes);
            respOS.flush();
            respOS.close();
        });
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    @Test
    public void testGet() throws IOException {
        try (CloseableHttpClient hc = HttpClientBuilder.create().build()) {
            int timeout = 1000;
            RequestConfig rc = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
            HttpGet get = new HttpGet("http://10.60.0.70:8000/");
            get.setConfig(rc);
            HttpResponse resp = hc.execute(get);
            Object[] resps = {
                resp.getStatusLine().getStatusCode(),
                headers2Map(resp.getAllHeaders()),
                EntityUtils.toString(resp.getEntity())};
            Stream.of(resps).forEach(System.out::println);
        }
    }
}
