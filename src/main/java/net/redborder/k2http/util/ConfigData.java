package net.redborder.k2http.util;

import java.util.LinkedList;
import java.util.List;

public class ConfigData {
    private static final String CONFIG_FILE_PATH = "/opt/rb/etc/k2http/config.yml";
    private static final ConfigFile configFile = new ConfigFile(CONFIG_FILE_PATH);
    private static final List<String> topics = new LinkedList<>();

    private ConfigData() {
    }


    public static String getZkConnect() {
        return configFile.getOrDefault("zk_connect", "127.0.0.1:2181");
    }

    public static List<String> getTopics() {
        topics.add("default_topic_1");
        topics.add("default_topic_2");
        return configFile.getOrDefault("topic", topics);
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

    public static void reload(){
        configFile.reload();
    }

    public static String currentConfig(){
        return  "{\"type\":\"config\"," +
                " \"zkConnect\":" + getZkConnect() + "," +
                " \"topic\":" + getTopics() + "," +
                " \"httpThreads\":" + getThreadNum() +
                " \"maxQueueSize\":" + getMaxQueueSize() +
                " \"endPoint\":" + getEndPoint() +
                "}";
    }
}
