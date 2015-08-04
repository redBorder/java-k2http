package net.redborder.k2http.http;

import net.redborder.k2http.util.ConfigData;
import net.redborder.k2http.util.Stats;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpWorker extends Thread {
    private String url;

    private final Integer okStatus = 200;
    private HttpClient client = HttpClientBuilder.create().build();
    private LinkedBlockingQueue<String> queue;
    private Logger log = LoggerFactory.getLogger(HttpWorker.class);


    public HttpWorker(LinkedBlockingQueue<String> queue, String endPoint, String topic) {
        this.url = endPoint + ConfigData.getUuid() + '/' + topic;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String msg;

            try {
                msg = queue.take();
                send(msg);
            } catch (InterruptedException e) {
                log.info("Shutting down");
            }
        }
    }

    private void send(String msg) {
        Integer retries = 1;
        HttpPost httpPost = new HttpPost(url);
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8)));
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        while (retries <= 10) {
            try {

                HttpResponse response = client.execute(httpPost);
                BufferedReader responseConnection = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent()));

                if ((response.getStatusLine().getStatusCode() == okStatus)) {
                    Stats.sent();
                    retries = okStatus;
                } else {
                    log.warn("#" + retries + " STATUS: " + response.getStatusLine().getStatusCode() +
                            "  -- MSG: " + org.apache.commons.io.IOUtils.toString(responseConnection));
                    log.debug("JSON: " + msg);
                    waitMoment();
                    retries++;
                }

                responseConnection.close();
            } catch (ClientProtocolException e) {
                waitMoment();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(retries != 200){
            Stats.drop();
        }

        httpPost.releaseConnection();
    }

    private void waitMoment() {
        Stats.retries();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }
}
