package com.adrienben.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DemoAppTest {

	@ClassRule
	public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, true, "dlq", "topic");

	@Autowired
	private DemoApp demoApp;

	private BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

	@BeforeClass
	public static void setup() {
		System.setProperty("spring.kafka.bootstrap-servers",
				broker.getEmbeddedKafka().getBrokersAsString());
	}

	@Before
	public void before() {
		demoApp.reset();
		records.clear();
	}

	@Test
	public void itShouldRetry() throws Exception {
		KafkaMessageListenerContainer<String, String> container = createAndStartContainer();

		KafkaTemplate<String, String> template = createTemplate();
		template.send("topic", "Foo");

		ConsumerRecord<String, String> record = records.poll(10, TimeUnit.SECONDS);
		container.stop();

		Assert.assertNotNull(record);
		Assert.assertEquals("Foo", record.value());
		Assert.assertEquals(DemoApp.MAX_ATTEMPTS, demoApp.getCounter().get());
	}

	@Test
	public void itShouldNotRetry() throws Exception {
		KafkaMessageListenerContainer<String, String> container = createAndStartContainer();

		KafkaTemplate<String, String> template = createTemplate();
		template.send("topic", "NotFoo");

		ConsumerRecord<String, String> record = records.poll(10, TimeUnit.SECONDS);
		container.stop();

		Assert.assertNotNull(record);
		Assert.assertEquals("NotFoo", record.value());
		Assert.assertEquals(1, demoApp.getCounter().get());
	}

	private KafkaMessageListenerContainer<String, String> createAndStartContainer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", broker.getEmbeddedKafka());
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties containerProperties = new ContainerProperties("dlq");
		KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setupMessageListener((MessageListener<String, String>) records::add);
		container.setBeanName("templateTests");
		container.start();
		ContainerTestUtils.waitForAssignment(container, broker.getEmbeddedKafka().getPartitionsPerTopic());
		return container;
	}

	private KafkaTemplate<String, String> createTemplate() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(broker.getEmbeddedKafka().getBrokersAsString());
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		return new KafkaTemplate<>(pf);
	}
}
