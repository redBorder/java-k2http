package net.redborder.k2http.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class Stats {
    private static Logger log = LoggerFactory.getLogger(Stats.class);
    private static AtomicLong receive = new AtomicLong();
    private static AtomicLong send = new AtomicLong();
    private static AtomicLong retries = new AtomicLong();
    private static AtomicLong lag = new AtomicLong();

    public static void received() {
        receive.incrementAndGet();
        lag.incrementAndGet();
    }

    public static void sent() {
        send.incrementAndGet();
        lag.decrementAndGet();
    }

    public static void retries(){
        retries.incrementAndGet();
    }

    public static void print() {
        long received = receive.getAndSet(0);
        long sent = send.getAndSet(0);
        long retry = send.getAndSet(0);

        log.info("{\"type\":\"stats\"," +
                " \"receive[msgs/s]\":"+(received / 15)+"" +
                ", \"send[msgs/s]\":" + (sent / 15) +
                ", \"lag[msgs]\":" + lag +
                ", \"retries\":" + (retry / 15 * 60) +
                "}" );
    }
}
