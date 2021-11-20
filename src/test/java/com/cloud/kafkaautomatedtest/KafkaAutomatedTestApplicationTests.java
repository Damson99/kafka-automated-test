package com.cloud.kafkaautomatedtest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.*;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {"server.port=0"}
)
public class KafkaAutomatedTestApplicationTests {

//	embedded kafka cluster
	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule=
		new EmbeddedKafkaRule(1, true, 1,
//				create input / output topics
				"input-topic", "output-topic");
//	kafka broker
	private static final EmbeddedKafkaBroker embeddedKafkaBroker=
			embeddedKafkaRule.getEmbeddedKafka();
//	kafka consumer
	private static Consumer<String, String> consumer;

//	kafka streams application
	@Autowired
	StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@BeforeClass
	public static void setUp() {

		Map<String, Object> consumerProps= consumerProps("group", "false", embeddedKafkaBroker);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> kafkaFactory=
				new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer= kafkaFactory.createConsumer();
		embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "output-topic");
	}


//	kafka producer - kafka template

	@Test
	public void processorApplicationTest() {

		Set<String> actualResultSet= new HashSet<>();
		Set<String> expectedResultSet= new HashSet<>();
		expectedResultSet.add("FIRST RECORD");
		expectedResultSet.add("SECOND RECORD");

		Map<String, Object> senderProps= producerProps(embeddedKafkaBroker);
		DefaultKafkaProducerFactory<Integer, String> kafkaProducerFactory=
				new DefaultKafkaProducerFactory<>(senderProps);
		try {

			KafkaTemplate<Integer, String> template=
					new KafkaTemplate<>(kafkaProducerFactory, true);
			template.setDefaultTopic("input-topic");

			template.sendDefault("first record");
			template.sendDefault("second record");

			int receivedAll= 0;
			while (receivedAll<2) {
				ConsumerRecords<String, String> consumerRecords =  getRecords(consumer);
				receivedAll= receivedAll+consumerRecords.count();
				consumerRecords.iterator().forEachRemaining(result ->
						actualResultSet.add(result.value()));
			}

			assertThat(actualResultSet.equals(expectedResultSet)).isTrue();
		}
		finally {
			kafkaProducerFactory.destroy();
		}
	}


	@AfterClass
	public static void tearDown() {

		consumer.close();
	}

}
