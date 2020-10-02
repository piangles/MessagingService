package org.piangles.backbone.services.msg;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.core.resources.ConsumerProperties;
import org.piangles.core.resources.KafkaMessagingSystem;
import org.piangles.core.resources.ResourceManager;

public class TestConsumer
{
	public static void main(String[] args) throws Exception
	{
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(new DefaultConfigProvider("MessagingService", "fd5f51bc-5a14-4675-9df4-982808bb106b"));
		ConsumerProperties consumerProps = new ConsumerProperties("group.id");
		consumerProps.getTopics().add(consumerProps.new Topic("", 1, 0));
		Consumer consumer = kms.createConsumer(consumerProps);

		while (true)
		{
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			System.out.println("Received : " + records.count());
			for (ConsumerRecord<String, String> record : records)
			{
				// print the offset,key and value for the consumer records.
				System.out.printf("offset = %d, key = %s, value = %s topic = %s\n", record.offset(), record.key(), record.value(), record.topic());
			}
		}
	}
}
