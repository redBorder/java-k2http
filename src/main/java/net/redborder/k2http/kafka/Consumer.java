package net.redborder.k2http.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.k2http.http.HttpManager;
import net.redborder.k2http.util.ConfigData;
import net.redborder.k2http.util.Stats;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class Consumer implements Runnable {
    private KafkaStream stream;
    private HttpManager httpManager;
    private ObjectMapper mapper;
    private List<Map<String, Object>> filters = ConfigData.getFilters();

    public Consumer(KafkaStream stream, HttpManager httpManager) {
        this.stream = stream;
        this.httpManager = httpManager;
        this.mapper = new ObjectMapper();
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
                boolean send = true;
                try {
                    Map<String, Object> message = mapper.readValue(msg, Map.class);
                    Map<String, Object> enrichment = (Map<String, Object>) message.get("enrichment");

                    for (Map<String, Object> filter : filters) {
                        for (Map.Entry<String, Object> filterEntry : filter.entrySet()) {
                            String key = filterEntry.getKey();
                            Object value = null;

                            if(enrichment != null) {
                                 value = enrichment.get(key);
                            }

                            if(value == null) {
                                value = message.get(key);
                            }

                            if (value == null || !value.equals(filterEntry.getValue())) {
                                send = false;
                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (send) {
                    httpManager.sendMsg(msg);
                }
            }
        }
    }
}
