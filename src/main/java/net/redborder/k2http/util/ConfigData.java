package net.redborder.k2http.util;

public class ConfigData {
    private static final String CONFIG_FILE_PATH = "/root/k2http.yml";
    private static final ConfigFile configFile = new ConfigFile(CONFIG_FILE_PATH);

    private ConfigData() {
    }

    public static String getZkConnect() {
        return configFile.getOrDefault("zk_connect", "127.0.0.1:2181");
    }

    public static String getTopic() {
        return configFile.getOrDefault("topic", "default_topic");
    }

    public static Integer getThreadNum() {
        return configFile.getOrDefault("httpThreadsNum", 5);
    }

    public static Integer getMaxQueueSize() {
        return configFile.getOrDefault("maxQueueSize", 10000);
    }

    public static String getEndPoint() {
        return configFile.getOrDefault("endpoint", "http://127.0.0.1/");
    }

    public static void reload(){
        configFile.reload();
    }

    public static String currentConfig(){
        return  "{\"type\":\"config\"," +
                " \"zkConnect\":" + getZkConnect() + "," +
                " \"topic\":" + getTopic() + "," +
                " \"httpThreads\":" + getThreadNum() +
                " \"maxQueueSize\":" + getMaxQueueSize() +
                " \"endPoint\":" + getEndPoint() +
                "}";
    }
}
