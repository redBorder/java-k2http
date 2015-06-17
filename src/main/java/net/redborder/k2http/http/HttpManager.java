package net.redborder.k2http.http;

import net.redborder.k2http.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HttpManager {
    private ExecutorService executor;
    private final String endPoint = ConfigData.getEndPoint();
    private LinkedBlockingQueue<String> queue;
    private Logger log = LoggerFactory.getLogger(HttpManager.class);
    volatile Boolean reloading = false;


    public HttpManager() {
        init();
    }

    private void init(){
        Integer threads = ConfigData.getThreadNum();
        this.executor = Executors.newFixedThreadPool(threads);
        this.queue = new LinkedBlockingQueue(ConfigData.getMaxQueueSize());

        for (Integer i = 0; i < threads; i++)
            executor.submit(new HttpWorker(queue, endPoint));
    }

    public void sendMsg(String msg) {
        if (msg != null) {
            try {
                if(!reloading) {
                    queue.put(msg);
                } else {
                    synchronized (reloading) {
                        reloading.wait();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void reload() {
        reloading = true;
        shutdown();
        init();
        synchronized (reloading) {
            reloading.notifyAll();
        }
        reloading = false;
    }

    public void shutdown() {
        Integer size = queue.size();
        log.info("Shutting down, queue size: " + size);

        while (size>0){
            try {
                Thread.sleep(5000);
                size = queue.size();
                log.info("Shutting down, queue size: " + size);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
