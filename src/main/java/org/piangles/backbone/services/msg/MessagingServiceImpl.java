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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.logging.LoggingService;
import org.piangles.backbone.services.msg.dao.MessagingDAO;
import org.piangles.backbone.services.msg.dao.MessagingDAOImpl;
import org.piangles.core.dao.DAOException;
import org.piangles.core.resources.KafkaMessagingSystem;
import org.piangles.core.resources.ResourceManager;
import org.piangles.core.util.abstractions.ConfigProvider;
import org.piangles.core.util.coding.JSON;

public class MessagingServiceImpl implements MessagingService
{
	private static final String COMPACT = "compact";
	private LoggingService logger = Locator.getInstance().getLoggingService();

	private MessagingDAO messagingDAO = null;
	private Properties msgProperties = null;
	private EntityConfiguration entityConfiguration = null;
	private Map<String, PartitionerAlgorithm> topicPartitionAlgoMap = null;

	private KafkaProducer<String, String> kafkaProducer = null;
	private Map<String, Topic> topicMap = null;

	public MessagingServiceImpl() throws Exception
	{
		messagingDAO = new MessagingDAOImpl();
		topicPartitionAlgoMap = messagingDAO.retrievePartitionerAlgorithmForTopics();
		
		ConfigProvider cp = new MsgConfigProvider(topicPartitionAlgoMap);
		
		msgProperties = cp.getProperties();
		entityConfiguration = new EntityConfiguration(msgProperties);
		
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(cp);
		kafkaProducer = kms.createProducer();
		
		topicMap = new HashMap<>();
	}

	@Override
	public void createTopicFor(String entityType, String entityId) throws MessagingException
	{
		logger.info("Creating topics for EntityType: " + entityType + " with Id: " + entityId);

		List<EntityProperties> listOfEntityProperties = entityConfiguration.getEntityProperties(entityType);
		if (listOfEntityProperties != null)
		{
			List<NewTopic> newTopics = new ArrayList<>();
			Admin adminClient = KafkaAdminClient.create(msgProperties);
			
			for (EntityProperties entityProperties : listOfEntityProperties)
			{
				String topicName = String.format(entityProperties.getTopicName(), entityId);

				Map<String, String> topicConfig = new HashMap<>();
				topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, entityProperties.getCleanupPolicy());
				topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(entityProperties.getRetentionPolicy()));

				NewTopic newTopic = new NewTopic(topicName, entityProperties.getNoOfPartitions(), entityProperties.getReplicationFactor());
				newTopic.configs(topicConfig);

				newTopics.add(newTopic);
			}

			CreateTopicsResult result = adminClient.createTopics(newTopics);

			KafkaFuture<Void> resultFuture = result.all();
			try
			{
				resultFuture.get();
			}
			catch (Exception e)
			{
				String message = "Failed to create Topic(s) for EntityType: " + entityType + " with EntityId: " + entityId + " because of: " + e.getMessage();
				logger.error(message, e);
				throw new MessagingException(message, e);
			}
			finally
			{
				try
				{
					adminClient.close();
				}
				catch (Exception e)
				{
					logger.error("Error closing AdminClient connection.");
				}
			}

			//Persist in EntityTable all the topics
			Topic topic = null;
			for (EntityProperties entityProperties : listOfEntityProperties)
			{
				String topicName = String.format(entityProperties.getTopicName(), entityId);
				boolean compacted = COMPACT.equals(entityProperties.getCleanupPolicy());
				
				topic = new Topic(topicName, entityProperties.getTopicPurpose(), entityProperties.getNoOfPartitions(), 
									compacted, entityProperties.shouldReadEarliest());
				try
				{
					messagingDAO.saveTopicsForEntity(entityType, entityId, topic);
				}
				catch (DAOException e)
				{
					logger.error("Failed Saving Topics: " + topic);
					String message = "Failed to save Topic(s) for EntityType: " + entityType + " with EntityId: " + entityId + " because of: " + e.getMessage();
					logger.error(message, e);
					throw new MessagingException(message, e);
				}
			}
			logger.info("Created topics for EntityType: " + entityType + " with EntityId: " + entityId + " successfuly.");
		}
		else
		{
			throw new MessagingException("No EntityConfiguration found for EntityType: " + entityType);
		}
	}

	/**
	 * All topics here are to be log compacted
	 */
	@Override
	public List<Topic> getTopicsFor(String entityType, String entityId) throws MessagingException
	{
		List<Topic> topics = null;
		logger.info("Retreving topics for EntityType: " + entityType + " with Id: " + entityId);
		try
		{
			topics = messagingDAO.retrieveTopicsForEntity(entityType, entityId);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForUser:", e);
			throw new MessagingException(e);
		}
		return topics;
	}

	@Override
	public Topic getTopic(String topicName) throws MessagingException
	{
		Topic topic = null;
		/**
		 * Only enable the logger for debugging way too many times this is
		 * logged especially when we have an process(engine) or service
		 * constantly publishing.
		 * 
		 * logger.debug("Retriving topic details for topic: " + topicName);
		 */
		try
		{
			synchronized (topicMap)
			{
				topic = topicMap.get(topicName);
				if (topic == null)
				{
					topic = messagingDAO.retrieveTopic(topicName);
					topicMap.put(topicName, topic);
				}
			}
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopic:", e);
			throw new MessagingException(e);
		}
		return topic;
	}

	/**
	 * Specifically called by Gateway on behalf of the clients connected to it.
	 * UI Clients will only know Aliases and this queries the tables msg.messaging_aliases
	 * meant for that purpose and returns the Topic details.
	 */
	@Override
	public List<Topic> getTopicsForAlias(String alias) throws MessagingException
	{
		List<Topic> topics = null;
		logger.info("Retriving topics for aliases: " + alias);
		try
		{
			topics = messagingDAO.retrieveTopicsForAliases(Arrays.asList(alias));
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForAliases:", e);
			throw new MessagingException(e);
		}
		return topics;
	}

	public void publish(String topicName, Event event) throws MessagingException
	{
		Topic topic = getTopic(topicName);
		if (topic != null)
		{
			kafkaProducer.send(createProducerRecord(topic, event), (metaData, expt) -> {
				if (expt != null)
				{
					logger.error("Unable to publish Event.", expt);
				}
			});
		}
		else
		{
			throw new MessagingException("TopicName: " + topicName + " is  not registered in MessagingService.");
		}
	}

	/**
	 * Distribute the message on all the topics listed.
	 * 
	 */
	@Override
	public void fanOut(FanoutRequest fanoutRequest) throws MessagingException
	{
		logger.info("FanoutRequest for Distribution Type:" + fanoutRequest.getDistributionListType() + "  and List:" + fanoutRequest.getDistributionList());
		List<Topic> topics = null;
		switch (fanoutRequest.getDistributionListType())
		{
		case Alias:
			try
			{
				topics = messagingDAO.retrieveTopicsForAliases(fanoutRequest.getDistributionList());
			}
			catch (DAOException e)
			{
				logger.error("Error retrieveTopicsForEntities : ", e);
				throw new MessagingException(e);
			}
			break;
		case Topic: // In this case the Custom Partioniner will kick in
			topics = fanoutRequest.getDistributionList().stream().map(topicName -> {
				try
				{
					return getTopic(topicName);
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}).collect(Collectors.toList());
			break;
		case Entity:
			try
			{
				topics = messagingDAO.retrieveTopicsForEntities(fanoutRequest.getEntityType(), fanoutRequest.getDistributionList());
			}
			catch (DAOException e)
			{
				logger.error("Error retrieveTopicsForEntities : ", e);
				throw new MessagingException(e);
			}
			break;
		}

		if (topics != null)
		{
			fanOut(topics, fanoutRequest.getEvent());
		}
		else
		{
			logger.warn("Topics could not be resolved for FanoutRequest Distribution Type:" + fanoutRequest.getDistributionListType() + "  and List:" + fanoutRequest.getDistributionList());
		}
	}
	
	public void shutdown()
	{
		try
		{
			kafkaProducer.close();
		}
		catch (Exception e)
		{
			logger.error("Error closing kafkaProducer connection.");
		}
	}

	private void fanOut(List<Topic> topics, Event event) throws MessagingException
	{
		topics.parallelStream().forEach(topic -> {
			try
			{
				kafkaProducer.send(createProducerRecord(topic, event), (metaData, expt) -> {
					if (expt != null)
					{
						logger.error("Unable to fanOut Event.", expt);
					}
				});
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		});
	}

	private ProducerRecord<String, String> createProducerRecord(Topic topic, Event event) throws MessagingException
	{
		ProducerRecord<String, String> record = null;

		String eventAsString = null;

		try
		{
			eventAsString = new String(JSON.getEncoder().encode(event));
		}
		catch (Exception e)
		{
			logger.error("Unable to encode Message.", e);
			throw new MessagingException(e);
		}

		if (topic.isCustomPartioned())
		{
			/**
			 * Configured CustomPartitioner will kick in and use 1. the
			 * primaryKey and 2. configured PartitionerAlgorithm to determine
			 * the PartitionNo.
			 */
			record = new ProducerRecord<>(topic.getTopicName(), event.getPrimaryKey(), eventAsString);
		}
		else // Either it is Default 0 or Topic has a specific parition
		{
			record = new ProducerRecord<>(topic.getTopicName(), topic.getPartition(), event.getPrimaryKey(), eventAsString);
		}

		return record;
	}
}
