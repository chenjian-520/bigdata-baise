package com.ict.bigdata.spark.job.sparkStreaming;

import java.io.Serializable;
import java.util.function.Consumer;

public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
}

