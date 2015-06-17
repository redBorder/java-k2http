package net.redborder.k2http.kafka;

import net.redborder.k2http.util.ConfigData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Topic {
    public final String name;
    public int partitions = 0;
    public int currentThreads = 0;
    CuratorFramework client;

    public Topic(String name) {
        client = CuratorFrameworkFactory.newClient(ConfigData.getZkConnect(), new RetryNTimes(10, 30000));
        client.start();
        this.name = name;
        refreshPartitions();
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

    public void close() {
        client.close();
    }
}
