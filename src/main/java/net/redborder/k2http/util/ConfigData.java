package net.redborder.k2http.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ConfigData {
    private static final String CONFIG_FILE_PATH = "/opt/rb/etc/k2http/config.yml";
    private static final ConfigFile configFile = new ConfigFile(CONFIG_FILE_PATH);
    private static List<String> topicsList = new LinkedList<>();
    private static Logger log = LoggerFactory.getLogger(ConfigData.class);

    private ConfigData() {
    }

    public static String getZkConnect() {
        return configFile.getOrDefault("zk_connect", "127.0.0.1:2181");
    }

    public static List<String> getTopics() {

        String topic = configFile.getOrDefault("topic", null);
        List<String> topics = configFile.getOrDefault("topics", null);

        if (topic != null) {
            log.warn("\"topic\" field on config.yml is deprecated");
        }

        if (!topics.isEmpty()) {
            topicsList = topics;
        } else if (topic != null) {
            topicsList.add(topic);
        }

        return topicsList;
    }

    public static Integer getThreadNum() {
        return configFile.getOrDefault("httpThreadsNum", 5);
    }

    public static Integer getMaxQueueSize() {
        return configFile.getOrDefault("maxQueueSize", 10000);
    }

    public static String getEndPoint() {
        return configFile.getOrDefault("endpoint", "http://127.0.0.1:8080/");
    }

    public static String getUuid() {
        return configFile.getOrDefault("uuid", null);
    }

    public static Boolean getSecurity() {
        return configFile.getOrDefault("insecure", false);
    }


    public static void reload() {
        configFile.reload();
    }

    public static String currentConfig() {
        return "{\"type\":\"config\"," +
                " \"zkConnect\":" + getZkConnect() + "," +
                " \"topic\":" + getTopics() + "," +
                " \"httpThreads\":" + getThreadNum() +
                " \"maxQueueSize\":" + getMaxQueueSize() +
                " \"endPoint\":" + getEndPoint() +
                "}";
    }
}
