package com.ict.bigdata.spark.job.sparkCore.service;


import com.ict.bigdata.spark.job.sparkStreaming.KafkaStreaming;
import scala.Serializable;

import java.util.Map;

public interface sparkService extends Serializable {

    <T> T execute(Map<String, Object> var) throws Exception;

    <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception;
}
