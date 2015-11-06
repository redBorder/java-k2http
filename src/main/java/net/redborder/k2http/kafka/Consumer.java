package net.redborder.k2http.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.util.ConfigData;
import net.redborder.k2http.util.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Consumer implements Runnable {
    private KafkaStream stream;
    private HttpManager httpManager;
    private ObjectMapper mapper;
    private List<Map<String, Object>> filters;
    private List<String> filtersMode;

    private Boolean filersEnabled = ConfigData.getFilterEnabled();
    private Logger log = LoggerFactory.getLogger(Consumer.class);

    public Consumer(String topicName, KafkaStream stream, HttpManager httpManager) {
        this.stream = stream;
        this.httpManager = httpManager;
        this.mapper = new ObjectMapper();
        this.filters = (List<Map<String, Object>>) ConfigData.getFilters().get(topicName);
        this.filtersMode = (List<String>) ConfigData.getFiltersMode().get(topicName);

        if (filtersMode == null) {
            filtersMode = Collections.singletonList("base");
        }

    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            Stats.received();
            String msg = null;
            try {
                msg = new String(it.next().message(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            if (msg != null) {
                boolean send = false;

                if (filersEnabled && filters != null) {
                    Map<String, Object> message = null;
                    try {
                        message = mapper.readValue(msg, Map.class);

                        if (filtersMode.contains("base")) {
                            send = baseFilter(message);
                        }

                        if (!send && filtersMode.contains("mse10")) {
                            send = mse10Filter(message);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        send = false;
                    }
                } else if (!filersEnabled) {
                    send = true;
                }

                log.debug("Send: {}", send);

                if (send) {
                    httpManager.sendMsg(msg);
                } else {
                    Stats.filtered();
                }
            }
        }
    }

    private boolean mse10Filter(Map<String, Object> message) {
        boolean send = false;
        boolean association = false;
        Map<String, Object> m = null;

        if (message.containsKey("notifications")) {
            List<Map<String, Object>> mList = (List<Map<String, Object>>) message.get("notifications");
            m = mList.get(0);
            String mType = (String) m.get("notificationType");
            if (mType != null && mType.equals("association")) {
                association = true;
            }
        }

        if (!association && m != null) {
            send = baseFilter(m);
        }

        return send;
    }

    private boolean baseFilter(Map<String, Object> message) {
        boolean send = false;

        Map<String, Object> enrichment = (Map<String, Object>) message.get("enrichment");

        for (Map<String, Object> filter : filters) {
            StringBuilder compare = new StringBuilder();
            StringBuilder data = new StringBuilder();

            for (Map.Entry<String, Object> filterEntry : filter.entrySet()) {
                String key = filterEntry.getKey();
                Object value = null;

                if (enrichment != null) {
                    value = enrichment.get(key);
                }

                if (value == null) {
                    value = message.get(key);
                }
                compare.append(filterEntry.getValue());
                data.append(value);
            }

            log.debug("{} - {}", compare, data);
            if (compare.toString().equals(data.toString())) {
                send = true;
            }

            if (send) {
                break;
            }
        }

        return send;
    }
}
