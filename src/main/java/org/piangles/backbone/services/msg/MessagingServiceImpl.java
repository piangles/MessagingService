package org.piangles.backbone.services.msg;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.logging.LoggingService;
import org.piangles.backbone.services.msg.dao.MessagingDAO;
import org.piangles.backbone.services.msg.dao.MessagingDAOImpl;
import org.piangles.core.dao.DAOException;
import org.piangles.core.resources.KafkaMessagingSystem;
import org.piangles.core.resources.ResourceManager;
import org.piangles.core.util.coding.JSON;

public class MessagingServiceImpl implements MessagingService
{
	private LoggingService logger = Locator.getInstance().getLoggingService();

	private MessagingDAO messagingDAO = null;
	private Map<String, PartitionerAlgorithm> topicPartitionAlgoMap = null;
	private KafkaProducer<String, String> kafkaProducer = null;

	public MessagingServiceImpl() throws Exception
	{
		messagingDAO = new MessagingDAOImpl();
		topicPartitionAlgoMap = messagingDAO.retrievePartitionerAlgorithmForTopics();
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(new MsgConfigProvider(topicPartitionAlgoMap));
		kafkaProducer = kms.createProducer();
	}

	@Override
	public Topic getTopic(String topicName) throws MessagingException
	{
		Topic topic = null;
		/**
		 * Only enable the logger for debugging way too many times this is logged
		 * especially when we have an process(engine) or service constantly publishing. 
		 * 
		 * logger.info("Retriving topic details for topic: " + topicName); 
		 */
		try
		{
			topic = messagingDAO.retrieveTopic(topicName);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopic:", e);
			throw new MessagingException(e);
		}
		return topic;
	}
	
	/**
	 * All topics here are to be log compacted
	 */
	@Override
	public List<Topic> getTopicsForUser(String userId) throws MessagingException
	{
		List<Topic> topics = null;
		logger.info("Retriving topics for user: " + userId);
		try
		{
			topics = messagingDAO.retrieveTopicsForEntity("UserId", userId);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForUser:", e);
			throw new MessagingException(e);
		}
		return topics;
	}

	/**
	 * Should all topics here be log compacted????
	 */
	@Override
	public List<Topic> getTopicsForAliases(List<String> aliases) throws MessagingException
	{
		List<Topic> topics = null;
		logger.info("Retriving topics for aliases: " + aliases);
		try
		{
			topics = messagingDAO.retrieveTopicsForAliases(aliases);
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
		kafkaProducer.send(createProducerRecord(getTopic(topicName), event), (metaData, expt) -> {
			if (expt != null)
			{
				logger.error("Unable to publish Event.", expt);
			}
		});
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
			topics = getTopicsForAliases(fanoutRequest.getDistributionList());
			break;
		case Topic: //In this case the Custom Partioniner will kick in
			topics = fanoutRequest.getDistributionList().stream().map(
					topicName -> {
						try
						{
							return getTopic(topicName);
						}
						catch (Exception e)
						{
							throw new RuntimeException(e);
						}
					}
			).collect(Collectors.toList());
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
			 * Configured CustomPartitioner will kick in and use 
			 * 1. the primaryKey 
			 * and 
			 * 2. configured PartitionerAlgorithm 
			 * to determine the PartitionNo.
			 */
			record = new ProducerRecord<>(topic.getTopicName(),
					event.getPrimaryKey(),eventAsString);
		}
		else //Either it is Default 0 or Topic has a specific parition
		{
			record = new ProducerRecord<>(topic.getTopicName(),topic.getPartition(),
					event.getPrimaryKey(),eventAsString);
		}
		
		return record;
	}

	@Override
	public void createStreamForUser(String userId) throws MessagingException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public Stream getStream() throws MessagingException
	{
		// TODO Auto-generated method stub
		return null;
	}
}
