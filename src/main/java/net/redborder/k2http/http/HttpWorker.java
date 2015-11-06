package net.redborder.k2http.http;

import net.redborder.k2http.util.ConfigData;
import net.redborder.k2http.util.Stats;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpWorker extends Thread {
    private String url;

    private final Integer okStatus = 200;

    private HttpClient client;
    private LinkedBlockingQueue<String> queue;
    private Logger log = LoggerFactory.getLogger(HttpWorker.class);


    public HttpWorker(LinkedBlockingQueue<String> queue, String endPoint, String topic) {

        if (ConfigData.isLegacyMode()) {
            this.url = endPoint;
        } else {
            this.url = endPoint + "/" + ConfigData.getUuid()  + topic;
        }

        this.queue = queue;

        try {
            init();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
    }

    private void init() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {

        if (ConfigData.getSecurity()) {
            SSLContext sslcontext = SSLContexts.custom()
                    .loadTrustMaterial(null, new TrustStrategy() {
                        public boolean isTrusted(
                                final X509Certificate[] chain, String authType) throws CertificateException {
                            // Oh, I am easy...
                            return true;
                        }
                    })
                    .build();

            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslcontext,
                    new String[]{"TLSv1"},
                    null,
                    new HostnameVerifier() {
                        public boolean verify(final String arg0, final SSLSession arg1) {
                            return true;
                        }
                    });

            client = HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } else {
            client = HttpClientBuilder.create().build();
        }
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

        while (retries <= 10) {
            try {
                BasicHttpEntity entity = new BasicHttpEntity();
                entity.setContent(new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8)));
                //entity.setContentLength(msg.length());
                entity.setContentType("application/json");
                httpPost.setEntity(entity);

                HttpResponse response = client.execute(httpPost);
                BufferedReader responseConnection = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent()));

                if ((response.getStatusLine().getStatusCode() == okStatus)) {
                    Stats.sent();
                    retries = okStatus;
                } else {
                    log.warn("#" + retries + " STATUS: " + response.getStatusLine().getStatusCode() +
                            "  -- URL: " + url +" MSG: " + org.apache.commons.io.IOUtils.toString(responseConnection));
                    log.debug("JSON: " + msg);
                    waitMoment();
                    retries++;
                }

                responseConnection.close();
            } catch (ClientProtocolException e) {
                waitMoment();
                log.error("Error: ", e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (retries != 200) {
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
