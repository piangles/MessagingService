package org.piangles.backbone.services.msg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.logging.LoggingService;

public final class CustomPartitioner implements Partitioner
{
	private LoggingService logger = Locator.getInstance().getLoggingService();
	private Map<String, ?> config = null;
	private AdminClient client = null;
	private Map<String, Integer> topicPartitionMap = null;
	
	@Override
	public void configure(Map<String, ?> config)
	{
		this.config = config;
		
		Properties props = new Properties();
		props.putAll(config);
		
 		client = AdminClient.create(props);

 		topicPartitionMap = new HashMap<>();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
	{
		int partition;
		
		PartitionerAlgorithm alogrithm = PartitionerAlgorithm.Default;
		try
		{
			alogrithm = PartitionerAlgorithm.valueOf((String)config.get(topic));
		}
		catch (Exception e)
		{
			logger.info("Exception parsing the PartitionerAlgorithm : ", e);	
		}
		
		switch (alogrithm)
		{
		case Deterministic:
			partition = deterministicPartition(topic, key.toString());
		case Derived:
			partition = derivedPartition(key.toString());
		default:
			partition = 0;
		}
		
		return partition;
	}

	@Override
	public void close()
	{
		client.close();
	}
	
	private int deterministicPartition(String topic, String key)
	{
		int partition = 0;
		
		Integer paritionCount = topicPartitionMap.get(topic);
		if (paritionCount == null)
		{
			paritionCount = 0;
	 		DescribeTopicsResult result = client.describeTopics(Arrays.asList(topic));
			Map<String, KafkaFuture<TopicDescription>>  values = result.values();
			
			KafkaFuture<TopicDescription> topicDescription = values.get(topic);
			
			try
			{
				paritionCount = topicDescription.get().partitions().size();
				topicPartitionMap.put(topic, paritionCount);
			}
			catch (Exception e)
			{
				logger.error("Could not retrieve paritionCount : ", e);
			}
		}

		try
		{
			Integer keyInt = Integer.parseInt(key.toString());
			if (paritionCount != 0)
			{
				partition = keyInt % paritionCount;
			}
		}
		catch (NumberFormatException e)
		{
			logger.error("Could not parse Key into Integer, defaulting to 0 : " + key, e);
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
			logger.error("Could not parse Key into Integer, defaulting to 0 : " + key, e);
		}
		return partition;
	}
}