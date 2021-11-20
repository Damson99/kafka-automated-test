package com.cloud.kafkaautomatedtest.binder;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

public interface ListenerBinder {

    @Input("process-in-0")
    KStream<String, String> inputStream();

    @Output("process-out-0")
    KStream<String, String> outStream();

}
