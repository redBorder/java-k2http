package net.redborder.k2http.http;

import net.redborder.k2http.thread.BlockingThreadPoolExecutor;
import net.redborder.k2http.thread.PriorityThreadFactory;
import net.redborder.k2http.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HttpManager {
    private ThreadPoolExecutor executor;
    private final String endPoint = ConfigData.getEndPoint();
    private Logger log = LoggerFactory.getLogger(HttpManager.class);

    public HttpManager() {
        Integer maxThreads = ConfigData.getThreadNum();
        this.executor = new BlockingThreadPoolExecutor(
                maxThreads,
                maxThreads,
                1L,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(ConfigData.getMaxQueueSize()), new PriorityThreadFactory("httpThreads"));
    }

    public void sendMsg(String msg) {
        if (msg != null) {
            executor.submit(new HttpWorker(endPoint, msg));
        }
    }

    public void reload() {
        Integer maxThreads = ConfigData.getThreadNum();
        if (executor.getMaximumPoolSize() < maxThreads) {
            executor.setMaximumPoolSize(maxThreads);
            executor.setCorePoolSize(maxThreads);
        } else {
            log.warn("The pool size is: " + executor.getMaximumPoolSize() + " you can't decrement it.");
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
