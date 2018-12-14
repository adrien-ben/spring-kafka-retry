package com.adrienben.demo;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Slf4j
@SpringBootApplication
public class DemoApp {

	public static final int MAX_ATTEMPTS = 5;

	@Getter
	private AtomicInteger counter = new AtomicInteger();

	@KafkaListener(id = "fooGroup", topics = "topic")
	public void listen(String foo) {
		log.info("Received Foo: {}", foo);
		counter.incrementAndGet();
		if ("Foo".equals(foo)) {
			throw new RetryableException("Retryable exception");
		}
		throw new RuntimeException("Exception");
	}

	public void reset() {
		counter.set(0);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory
	) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setStatefulRetry(true);
		factory.setRetryTemplate(retryTemplate());
		return factory;
	}

	private RetryTemplate retryTemplate() {
		RetryTemplate template = new RetryTemplate();

		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		template.setBackOffPolicy(backOffPolicy);

		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(MAX_ATTEMPTS,
				Collections.singletonMap(RetryableException.class, true),
				true);
		template.setRetryPolicy(retryPolicy);
		return template;
	}

	@Bean
	public ErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
				(r, e) -> new TopicPartition("dlq", r.partition()));
		return new ExtendedSeekToCurrentErrorHandler(recoverer, MAX_ATTEMPTS,
				Collections.singletonMap(RetryableException.class, true), true);
	}

	private static class ExtendedSeekToCurrentErrorHandler extends SeekToCurrentErrorHandler {

		private final BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer;
		private final BinaryExceptionClassifier retryableClassifier;

		public ExtendedSeekToCurrentErrorHandler(
				BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
				int maxFailures,
				Map<Class<? extends Throwable>, Boolean> retryableExceptions,
				boolean traverseCauses
		) {
			super(recoverer, maxFailures);
			this.recoverer = recoverer;
			this.retryableClassifier = new BinaryExceptionClassifier(retryableExceptions, false);
			this.retryableClassifier.setTraverseCauses(traverseCauses);
		}

		@Override
		public void handle(
				Exception thrownException,
				List<ConsumerRecord<?, ?>> records,
				Consumer<?, ?> consumer,
				MessageListenerContainer container
		) {
			if (isRetryable(thrownException)) {
				super.handle(thrownException, records, consumer, container);
			} else if (!records.isEmpty()) {
				recoverer.accept(records.get(0), thrownException);
			}
		}

		private boolean isRetryable(Throwable throwable) {
			return retryableClassifier.classify(throwable);
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoApp.class, args);
	}
}
