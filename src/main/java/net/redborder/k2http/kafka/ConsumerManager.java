package net.redborder.k2http.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.redborder.clusterizer.ZkTasksHandler;
import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.util.ConfigData;
import net.redborder.k2http.util.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerManager extends Thread {
    private Logger log = LoggerFactory.getLogger(ConsumerManager.class);
    private static ConsumerConnector consumer;
    private ExecutorService executor;
    private ZkTasksHandler clusterizer;
    private HttpManager manager;
    private Topic topic;
    private int numWorkers = 0;
    private Properties props = new Properties();
    volatile private Status status = Status.STOPPED;

    public enum Status {
        INIT, ACTIVE, STOPPED;
    }


    public ConsumerManager(HttpManager manager) {
        status = Status.INIT;
        String zk = ConfigData.getZkConnect();
        log.info("Initiate ConsumerManager using zk: " + zk);
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", zk);
        props.put("group.id", "r2http");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");
        clusterizer = new ZkTasksHandler(zk, "/rb-k2http");
        this.manager = manager;
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        topic = new Topic(ConfigData.getTopic());
        calculeThreads();
    }

    @Override
    public void run() {
        status = Status.ACTIVE;
        rebalance();
        log.info("Status[ " + status.name() + " ]");
        while (status.equals(Status.ACTIVE)) {
            Boolean needRebalance = isNeedRebalance();

            log.debug("Waking up, need to rebalance: " + needRebalance);
            if (needRebalance) {
                rebalance();
            }

            Stats.print();
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
            }
        }
    }

    private Boolean isNeedRebalance() {
        Boolean isNeedRebalance = false;
        if (topic.currentThreads != calculeThreads()) {
            isNeedRebalance = true;
        }

        return isNeedRebalance;
    }

    private void rebalance() {
        if (executor != null && consumer != null) {
            executor.shutdown();
            consumer.shutdown();
            executor = null;
            consumer = null;
        }

        calculeThreads();
        log.info("{\"type\":\"rebalancing\", \"workers\":" + numWorkers + ", \"partitions\":" + topic.partitions + ", \"threads\":" + topic.currentThreads + "}");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topic.toMap());

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic.name);
        executor = Executors.newFixedThreadPool(topic.currentThreads);

        for (final KafkaStream stream : streams) {
            executor.submit(new Consumer(stream, manager));
        }

        log.info("Ended rebalance.");
    }

    public void shutdown() {
        log.info("Shutdown Consumer Manager ...");
        status = Status.STOPPED;
        clusterizer.end();
        topic.close();
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        log.info("Status[ " + status.name() + " ]");
    }

    public void reload() {
        rebalance();
    }

    private Integer calculeThreads() {
        Boolean recalculate = false;
        int currentWorkers = clusterizer.numWorkers();

        if (numWorkers == 0 || numWorkers != currentWorkers) {
            numWorkers = currentWorkers < 1 ? 1 : currentWorkers;
            recalculate = true;
        }

        if (topic.refreshPartitions()) {
            recalculate = true;
        }

        if (recalculate) {
            Float threads = (float) topic.getPartitions() / numWorkers;

            if (clusterizer.isLeader()) {
                topic.currentThreads = new Double(Math.ceil(threads)).intValue();
            } else {
                topic.currentThreads = new Double(Math.floor(threads)).intValue();
            }
        }

        log.debug("Calculating threads (recalculate " + recalculate + "):");
        log.debug("  * Current workers: " + numWorkers);
        log.debug("  * Current partitions: " + topic.partitions);
        log.debug("  * Current threads: " + topic.currentThreads);


        return topic.currentThreads;
    }
}
