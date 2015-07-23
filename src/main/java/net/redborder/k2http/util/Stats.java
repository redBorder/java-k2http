package net.redborder.k2http.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class Stats {
    private static Logger log = LoggerFactory.getLogger(Stats.class);
    private static AtomicLong receive = new AtomicLong();
    private static AtomicLong send = new AtomicLong();
    private static AtomicLong retries = new AtomicLong();
    private static AtomicLong drop = new AtomicLong();
    private static AtomicLong lag = new AtomicLong();

    public static void received() {
        receive.incrementAndGet();
        lag.incrementAndGet();
    }

    public static void sent() {
        send.incrementAndGet();
        lag.decrementAndGet();
    }

    public static void drop() {
        drop.incrementAndGet();
        lag.decrementAndGet();
    }

    public static void retries() {
        retries.incrementAndGet();
    }

    public static void print() {
        long received = receive.getAndSet(0);
        long sent = send.getAndSet(0);
        long retry = retries.getAndSet(0);
        long lagg = lag.get();
        long dropped = drop.getAndSet(0);
        long threads = Integer.valueOf(ConfigData.getThreadNum()).longValue();

        if (threads > lagg) {
            threads = lagg;
            lagg = 0;
        } else {
            lagg = lagg - threads;
        }

        log.info("{\"type\":\"stats\"," +
                " \"receive[msgs/s]\":" + (received / 60) + "" +
                ", \"send[msgs/s]\":" + (sent / 60) +
                ", \"lag[msgs]\":" + lagg +
                ", \"busyThreads\":" + threads +
                ", \"retries\":" + (retry) +
                ", \"dropped\":" + (dropped) +
                "}");
    }
}
