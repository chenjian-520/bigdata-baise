package com.ict.bigdata.spark.job.sparkStreaming;

import com.ict.bigdata.spark.job.SparkApp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import com.ict.bigdata.spark.job.sparkStreaming.domain.DPKafkaInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaStreaming implements Serializable {
    private Map kafkaParams;
    private Collection topics;
    static JavaInputDStream<ConsumerRecord<String, Object>> dStream;
    static JavaDStream<ConsumerRecord<String, Object>> windowDStream;

    public static final KafkaStreaming getKafkaStreaming() {
        return KafkaStreamingInstance.INSTANCE;
    }

    public KafkaStreaming() {
    }

    public void init() {
        DPKafkaInfo dpKafkaInfo = SparkApp.getDpPermissionManager().initialKafkaSecurityContext();
        this.kafkaParams = new HashMap();
        this.kafkaParams.put("bootstrap.servers", dpKafkaInfo.getServerUrl());
        this.kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        this.kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        this.kafkaParams.put("key.serializer", StringSerializer.class.getName());
        this.kafkaParams.put("value.serializer", StringSerializer.class.getName());
        this.kafkaParams.put("group.id", dpKafkaInfo.getGroupId());
        this.kafkaParams.put("auto.offset.reset", "earliest");
        this.kafkaParams.put("enable.auto.commit", "false");
        this.kafkaParams.put("max.poll.interval.ms", "120000");
        this.topics = Arrays.asList(dpKafkaInfo.getTopics().split(","));
        if (dpKafkaInfo.getKafkaConf() != null) {
            this.kafkaParams.putAll(dpKafkaInfo.getKafkaConf());
        }
    }

    public void startJob(SerializableConsumer<JavaRDD<ConsumerRecord<String, Object>>> streamrddConsumer) throws Exception {
        DPKafkaInfo kafkaInfo = SparkApp.getDPKafkaInfo();
        JavaStreamingContext scc = new JavaStreamingContext(SparkApp.getContext(), Durations.seconds(kafkaInfo.getBatchDuration()));
        dStream = KafkaUtils.createDirectStream(scc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(this.topics, this.kafkaParams));
        if (kafkaInfo.getWindowDurationMultiple() != null) {
            Duration windowDuration = Durations.seconds(kafkaInfo.getWindowDurationMultiple() * kafkaInfo.getBatchDuration());
            Duration sliverDuration = Durations.seconds(kafkaInfo.getSliverDurationMultiple() * kafkaInfo.getBatchDuration());
            windowDStream = dStream.map(new Function<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>>() {
                public ConsumerRecord<String, Object> call(ConsumerRecord<String, Object> kafkaRecord) throws Exception {
                    return kafkaRecord;
                }
            }).window(windowDuration, sliverDuration);
            AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();
            dStream.foreachRDD((rdd) -> {
                if (!rdd.isEmpty()) {
                    OffsetRange[] alloffsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(alloffsetRanges);
                }
            });
            windowDStream.foreachRDD((rdd) -> {
                if (!rdd.isEmpty()) {
                    streamrddConsumer.accept(rdd);
                    ((CanCommitOffsets) dStream.inputDStream()).commitAsync((OffsetRange[]) offsetRanges.get());
                }
            });
        } else {
            dStream.foreachRDD((rdd) -> {
                if (!rdd.isEmpty()) {
                    streamrddConsumer.accept(rdd);
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    ((CanCommitOffsets) dStream.inputDStream()).commitAsync(offsetRanges);
                }
            });
        }
        scc.start();
        scc.awaitTermination();
    }

    public Map getKafkaParams() {
        return this.kafkaParams;
    }

    public Collection getTopics() {
        return this.topics;
    }

    private static class KafkaStreamingInstance {
        private static final KafkaStreaming INSTANCE = new KafkaStreaming();

        private KafkaStreamingInstance() {
        }
    }
}
