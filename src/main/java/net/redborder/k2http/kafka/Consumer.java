package net.redborder.k2http.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.k2http.util.Stats;

import java.util.concurrent.LinkedBlockingQueue;

public class Consumer implements Runnable {
    private KafkaStream stream;
    private LinkedBlockingQueue<String> msgQueue;

    public Consumer(KafkaStream stream, LinkedBlockingQueue<String> msgQueue) {
        this.stream = stream;
        this.msgQueue = msgQueue;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            try {
                msgQueue.put(new String(it.next().message()));
                Stats.received();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
