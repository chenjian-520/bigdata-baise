package com.example.bigdataSpark.sparkJob.sparkCore.service;

import scala.Serializable;

import java.util.Map;

public interface sparkService extends Serializable {
 <T> T execute(Map<String, Object> var) throws Exception;
}
