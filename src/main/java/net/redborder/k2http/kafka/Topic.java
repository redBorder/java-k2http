package net.redborder.k2http.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.util.ConfigData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;

import net.redborder.clusterizer.ZkTasksHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Topic extends Thread {
    public final String name;
    private ExecutorService executor;
    volatile private Status status = Status.STOPPED;
    public int partitions = 0;
    public int currentThreads = 0;
    private int numWorkers = 0;
    private HttpManager manager;
    private static ConsumerConnector consumer;
    private Logger log = LoggerFactory.getLogger(ConsumerManager.class);
    private ZkTasksHandler clusterizer;
    CuratorFramework client;

    private Properties props;

    public enum Status {
        INIT, ACTIVE, STOPPED;
    }

    public Topic(String name, HttpManager manager, Properties props, ZkTasksHandler clusterizer) {
        this.name = name;
        this.manager = manager;
        this.props = props;
        this.clusterizer = clusterizer;
        status = Status.INIT;
        client = CuratorFrameworkFactory.newClient(ConfigData.getZkConnect(), new RetryNTimes(10, 30000));
        client.start();
        refreshPartitions();
        calculeThreads();
        System.out.println("Creando topic: " + name);
    }

    public void run() {
        status = Status.ACTIVE;
        rebalance();
        log.info(this.name + ": Status[ " + status.name() + " ]");

        while (status.equals(Status.ACTIVE)) {
            Boolean needRebalance = this.isNeedRebalance();

            log.debug(this.name + ": Waking up, need to rebalance: " + needRebalance);

            if (needRebalance) {
                this.rebalance();
            }

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    public Integer getPartitions() {
        return partitions;
    }

    public Map<String, Integer> toMap() {
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, currentThreads);
        return hash;
    }

    public Boolean refreshPartitions() {
        int currentPartitions = partitions;
        List<String> partitionsList;
        try {
            if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                partitionsList = client.getChildren().forPath("/brokers/topics/" + name + "/partitions");
                partitions = partitionsList.size();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partitions != currentPartitions;
    }

    private Integer calculeThreads() {
        Boolean recalculate = false;
        int currentWorkers = clusterizer.numWorkers();

        if (numWorkers == 0 || numWorkers != currentWorkers) {
            numWorkers = currentWorkers < 1 ? 1 : currentWorkers;
            recalculate = true;
        }

        if (this.refreshPartitions()) {
            recalculate = true;
        }

        if (recalculate) {
            System.out.println("Recalculando");
            Float threads = (float) getPartitions() / numWorkers;

            if (clusterizer.isLeader()) {
                currentThreads = new Double(Math.ceil(threads)).intValue();
            } else {
                currentThreads = new Double(Math.floor(threads)).intValue();
            }
        }

        log.debug("Calculating threads (recalculate " + recalculate + "):");
        log.debug("  * Current workers: " + numWorkers);
        log.debug("  * Current partitions: " + partitions);
        log.debug("  * Current threads: " + currentThreads);

        return currentThreads;
    }

    public Boolean isNeedRebalance() {
        Boolean isNeedRebalance = false;
        if (currentThreads != calculeThreads()) {
            isNeedRebalance = true;
        }

        return isNeedRebalance;
    }

    public void rebalance() {
        if (executor != null && consumer != null) {
            executor.shutdown();
            consumer.shutdown();
            executor = null;
            consumer = null;
        }

        calculeThreads();
        log.info("{\"type\":\"rebalancing\", \"workers\":" + numWorkers + ", \"partitions\":" + partitions + ", \"threads\":" + currentThreads + "}");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(toMap());

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.name);
        executor = Executors.newFixedThreadPool(currentThreads);

        for (final KafkaStream stream : streams) {
            executor.submit(new Consumer(stream, manager));
        }

        log.info("Ended rebalance.");
    }

    public void close() {
        client.close();
    }

    public void shutdown() {
        log.info("Shutdown Consumer Manager ...");
        status = Status.STOPPED;
        clusterizer.end();
        close();
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        log.info("Status[ " + status.name() + " ]");
    }
}
