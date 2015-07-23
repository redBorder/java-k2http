package net.redborder.k2http.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.util.Stats;

import java.io.UnsupportedEncodingException;

public class Consumer implements Runnable {
    private KafkaStream stream;
    private HttpManager httpManager;

    public Consumer(KafkaStream stream, HttpManager httpManager) {
        this.stream = stream;
        this.httpManager = httpManager;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            Stats.received();
            String msg = null;
            try {
                msg = new String(it.next().message(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            httpManager.sendMsg(msg);
        }
    }
}
