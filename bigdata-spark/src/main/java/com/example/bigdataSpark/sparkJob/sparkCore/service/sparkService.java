package com.example.bigdataSpark.sparkJob.sparkCore.service;

import com.example.bigdataSpark.sparkJob.sparkStreaming.KafkaStreaming;
import scala.Serializable;

import java.util.Map;

public interface sparkService extends Serializable {

    <T> T execute(Map<String, Object> var) throws Exception;

    <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception;
}
