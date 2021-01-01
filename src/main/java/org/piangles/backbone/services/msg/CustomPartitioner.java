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

import static org.piangles.backbone.services.msg.Constants.PARTITION_ALGORITHM;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.Utils;
import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.logging.LoggingService;

public final class CustomPartitioner implements Partitioner
{
	private LoggingService logger = Locator.getInstance().getLoggingService();
	private Map<String, ?> kafkaConfig = null;
	private AdminClient adminClient = null;
	private Map<String, Integer> topicPartitionMap = null;
	
	@Override
	public void configure(Map<String, ?> config)
	{
		this.kafkaConfig = config;
		
		Properties props = new Properties();
		props.putAll(config);
		
 		adminClient = AdminClient.create(props);

 		topicPartitionMap = new HashMap<>();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
	{
		int partition;
		PartitionerAlgorithm alogrithm = (PartitionerAlgorithm)kafkaConfig.get(PARTITION_ALGORITHM + topic);
		if (alogrithm == null)
		{
			logger.warn("Using Default PartitionerAlgorithm because No algorithm was configured for partitioning for: " + topic);
			alogrithm = PartitionerAlgorithm.Default;
		}
		switch (alogrithm)
		{
		case Deterministic:
			partition = deterministicPartition(topic, key.toString());
			break;
		case Derived:
			partition = derivedPartition(key.toString());
			break;
		default:
			partition = 0;
			break;
		}
		
		return partition;
	}

	@Override
	public void close()
	{
		adminClient.close();
	}
	
	private int deterministicPartition(String topic, String key)
	{
		int partition = 0;
		
		Integer paritionCount = topicPartitionMap.get(topic);
		if (paritionCount == null)
		{
	 		DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topic));
			Map<String, KafkaFuture<TopicDescription>>  values = result.values();
			
			KafkaFuture<TopicDescription> topicDescription = values.get(topic);
			
			try
			{
				paritionCount = topicDescription.get().partitions().size();
				topicPartitionMap.put(topic, paritionCount);
			}
			catch (Exception e)
			{
				paritionCount = 0;
				logger.error("Could not retrieve paritionCount(Defaulting to 0) : ", e);
			}
		}

		if (paritionCount != 0)
		{
			partition = Utils.toPositive(Utils.murmur2(key.getBytes())) % paritionCount;
		}
		
		return partition;
	}
	
	private int derivedPartition(String key)
	{
		int partition = 0;
		try
		{
			Integer keyInt = Integer.parseInt(key);
			partition = keyInt;
		}
		catch (NumberFormatException e)
		{
			logger.error("Could not parse Key into Integer(Defaulting to 0) : " + key, e);
		}
		return partition;
	}
}
