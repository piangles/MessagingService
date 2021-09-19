/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
 
 
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
		consumerProps.getTopics().add(consumerProps.new Topic("", 1, false));
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
