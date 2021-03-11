package com.example.bigdataSpark.kafka.controller;

import com.example.bigdataSpark.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaController {
    private final static Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private KafkaService kafkaService;

    @RequestMapping(value = "/{topic}/send", method = RequestMethod.GET)
    public void sendMeessageTotopic1(@PathVariable String topic) {
        logger.info("start send message to {}", topic);
        kafkaService.sendMessage(topic, "你好");
    }
}