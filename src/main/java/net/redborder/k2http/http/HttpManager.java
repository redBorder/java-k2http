package net.redborder.k2http.http;

import net.redborder.k2http.thread.BlockingThreadPoolExecutor;
import net.redborder.k2http.thread.PriorityThreadFactory;
import net.redborder.k2http.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HttpManager extends Thread {
    private LinkedBlockingQueue<String> queue;
    private ThreadPoolExecutor executor;
    private final String endPoint = ConfigData.getEndPoint();
    volatile private Status status = Status.STOPPED;
    private Logger log = LoggerFactory.getLogger(HttpManager.class);
    private final Object object = new Object();

    public enum Status {
        INIT, ACTIVE, STOPPED, RELOAD;
    }

    public HttpManager(LinkedBlockingQueue<String> queue) {
        status = Status.INIT;
        this.queue = queue;
        Integer maxThreads = ConfigData.getThreadNum();
        this.executor = new BlockingThreadPoolExecutor(
                maxThreads,
                maxThreads,
                1L,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(ConfigData.getMaxQueueSize()), new PriorityThreadFactory("httpThreads"));
        log.info("Status[ " + status.name() + " ]");
    }

    @Override
    public void run() {
        status = Status.ACTIVE;
        log.info("Status[ " + status.name() + " ]");
        while (status.equals(Status.ACTIVE)) {
            String msg = null;
            try {
                msg = queue.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (status.equals(Status.RELOAD)) {
                reloading();
            }

            if (msg != null) {
                executor.submit(new HttpWorker(endPoint, msg));
            } else if (status.equals(Status.STOPPED)) {
                try {
                    executor.shutdown();
                    executor.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void reload() {
        try {
            this.executor.shutdown();
            this.executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Integer maxThreads = ConfigData.getThreadNum();
        this.executor = new BlockingThreadPoolExecutor(
                maxThreads,
                maxThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(ConfigData.getMaxQueueSize()), new PriorityThreadFactory("httpThreads"));
        synchronized (object) {
            object.notifyAll();
        }
    }

    public void shutdown() {
        status = Status.STOPPED;
        log.info("Status[ " + status.name() + " ]");
    }

    private void reloading() {
        synchronized (object) {
            try {
                object.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
