package org.piangles.backbone.services.msg;

import java.util.List;
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
	private KafkaProducer<String, String> kafkaProducer = null;

	public MessagingServiceImpl() throws Exception
	{
		messagingDAO = new MessagingDAOImpl();
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(new MsgConfigProvider(
																	messagingDAO.retrievePartitionerAlgorithmForTopics()));
		kafkaProducer = kms.createProducer();
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
	
	public void publish(String topic, Event event) throws MessagingException
	{
		final String eventAsStr = encodeEvent(event);
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getPrimaryKey(),eventAsStr);
		kafkaProducer.send(record, (metaData, expt) -> {
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
			topics = fanoutRequest.getDistributionList().stream().map(topicStr -> new Topic(topicStr)).collect(Collectors.toList());
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
		final String eventAsStr = encodeEvent(event);
		topics.parallelStream().forEach(topic -> {
			ProducerRecord<String, String> record = null;
			if (topic.isPartioned())
			{
				record = new ProducerRecord<>(topic.getTopicName(),topic.getPartition(),
						event.getPrimaryKey(),eventAsStr);
			}
			else //CustomPartitioner will kick in and use the primaryKey to determine the Partition
			{
				record = new ProducerRecord<>(topic.getTopicName(),
						event.getPrimaryKey(),eventAsStr);
			}
			
			kafkaProducer.send(record, (metaData, expt) -> {
				if (expt != null)
				{
					logger.error("Unable to fanOut Event.", expt);
				}
			});
		});
	}
	
	private String encodeEvent(Event event) throws MessagingException
	{
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
		
		return eventAsString;
	}
}
