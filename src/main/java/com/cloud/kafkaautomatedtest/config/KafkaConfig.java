package com.cloud.kafkaautomatedtest.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

@Slf4j
@Configuration
@EnableAutoConfiguration
public class KafkaConfig {

//    functional programming with kafka streams. Spring will create automatically map topics from yaml
    @Bean
    public Function<KStream<String, String>, KStream<String, String>> process() {

        return input -> input.mapValues((ValueMapper<String, String>) String::toUpperCase);
    }
}
