package net.redborder.k2http.http;

import net.redborder.k2http.util.Stats;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HttpWorker implements Runnable {
    private String msg;
    private String endPoint;

    private final Integer okStatus = 200;
    private boolean retry = true;

    public HttpWorker(String endPoint, String msg) {
        this.endPoint = endPoint;
        this.msg = msg;
    }

    @Override
    public void run() {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        HttpPost httpPost = new HttpPost(endPoint);
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8)));
        entity.setContentType("application/json");
        httpPost.setEntity(entity);

        try {
            while (retry) {
                CloseableHttpResponse response = httpclient.execute(httpPost);

                if ((response.getStatusLine().getStatusCode() == okStatus)) {
                    retry = false;
                    Stats.sent();
                } else {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                response.close();
            }
            httpclient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
