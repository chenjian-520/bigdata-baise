package com.example.bigdataSpark.sparkJob.service;

import scala.Serializable;

public interface sparkService extends Serializable {
 <T> T execute() throws Exception;
}
