package net.redborder.k2http.kafka;

import net.redborder.clusterizer.ZkTasksHandler;
import net.redborder.k2http.util.ConfigData;

import java.util.*;

public class ConsumerManager {
    private List<Topic> topics;
    private Properties props = new Properties();
    private String zk = ConfigData.getZkConnect();
    private ZkTasksHandler clusterizer;

    public ConsumerManager() {

        int n_topics = ConfigData.getTopics().size();

        clusterizer = new ZkTasksHandler(zk, "/rb-k2http");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", zk);
        props.put("group.id", "k2http10");
        props.put("zookeeper.session.timeout.ms", "5000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");

        topics = new LinkedList<>();

        for (int i = 0; i < n_topics; i++) {
            topics.add(new Topic(ConfigData.getTopics().get(i), props, clusterizer));
        }
    }

    public void start() {
        Iterator<Topic> iterator = topics.iterator();

        while (iterator.hasNext()) {
            iterator.next().start();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void reload() {
        Iterator<Topic> iterator = topics.iterator();

        while (iterator.hasNext()) {
            iterator.next().reload();
        }
    }

    public void shutdown() {
        Iterator<Topic> iterator = topics.iterator();
        clusterizer.end();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
        }
    }
}
