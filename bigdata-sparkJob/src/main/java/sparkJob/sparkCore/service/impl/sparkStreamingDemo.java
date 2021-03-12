package sparkJob.sparkCore.service.impl;

import sparkJob.sparkCore.service.sparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Map;

public class sparkStreamingDemo implements sparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        return null;
    }
}
