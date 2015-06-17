package net.redborder.k2http;

import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.kafka.ConsumerManager;
import net.redborder.k2http.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import java.util.concurrent.LinkedBlockingQueue;

public class K2Http {
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(K2Http.class);
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(ConfigData.getMaxQueueSize());

        final HttpManager httpManager = new HttpManager(queue);
        final ConsumerManager consumerManager = new ConsumerManager(queue);

        consumerManager.start();
        httpManager.start();

        log.info(ConfigData.currentConfig());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Exiting...");
                consumerManager.shutdown();
                httpManager.shutdown();
            }
        });

        // Add signal to reload config
        Signal.handle(new Signal("HUP"), new SignalHandler() {
            public void handle(Signal signal) {
                log.info("Reload received!");
                // Reload the config file
                ConfigData.reload();
                consumerManager.reload();
                httpManager.reload();
                log.info("Reload finished!");
                log.info(ConfigData.currentConfig());
            }
        });
    }
}
