package com.cloud.kafkaautomatedtest.service;

import com.cloud.kafkaautomatedtest.binder.ListenerBinder;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@EnableBinding(ListenerBinder.class)
public class KafkaService {

    @StreamListener("process-in-0")
    @SendTo("process-out-0")
    public KStream<String, String> process(KStream<String, String> input) {

        input.foreach((k, v) -> log.info("Received Input: {}",v));
        return input.mapValues((ValueMapper<String, String>) String::toUpperCase);

    }
}
