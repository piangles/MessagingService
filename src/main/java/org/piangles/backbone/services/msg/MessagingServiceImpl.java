package org.piangles.backbone.services.msg;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.backbone.services.logging.LoggingService;
import org.piangles.backbone.services.msg.dao.MessagingDAO;
import org.piangles.backbone.services.msg.dao.MessagingDAOImpl;
import org.piangles.core.dao.DAOException;
import org.piangles.core.resources.KafkaMessagingSystem;
import org.piangles.core.resources.ResourceManager;
import org.piangles.core.util.coding.JSON;

public class MessagingServiceImpl implements MessagingService
{
	private static final String COMPONENT_ID = "fd5f51bc-5a14-4675-9df4-982808bb106b";
	private LoggingService logger = Locator.getInstance().getLoggingService();

	private MessagingDAO controlChannelDAO = null;
	private KafkaProducer<String, String> kafkaProducer = null;

	public MessagingServiceImpl() throws Exception
	{
		controlChannelDAO = new MessagingDAOImpl();
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(new DefaultConfigProvider("MessagingService", COMPONENT_ID));
		kafkaProducer = kms.createProducer();
	}

	/**
	 * All topics here are to be log compacted
	 */
	@Override
	public List<Topic> getTopicsFor(String userId) throws MessagingException
	{
		List<Topic> topics = null;
		logger.info("Retriving topics for user: " + userId);
		try
		{
			topics = controlChannelDAO.retrieveTopicsForUser(userId);
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
			topics = controlChannelDAO.retrieveTopicsForAliases(aliases);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForAliases:", e);
			throw new MessagingException(e);
		}
		return topics;
	}

	/**
	 * Distribute the message on all the topics listed.
	 * 
	 */
	@Override
	public void fanOut(FanoutRequest fanoutRequest) throws MessagingException
	{
		List<Topic> topics = null;
		if (fanoutRequest.getEntityIdType() == EntityIdType.Alias)
		{
			topics = getTopicsForAliases(fanoutRequest.getEntityIds());
		}
		else
		{
			topics = fanoutRequest.getEntityIds().stream().map(topicStr -> new Topic(topicStr)).collect(Collectors.toList());
		}

		Message message = fanoutRequest.getMessage();
		String msgAsString = null;
		try
		{
			msgAsString = new String(JSON.getEncoder().encode(message));
		}
		catch (Exception e)
		{
			logger.error("Unable to encode Message.", e);
			throw new MessagingException(e);
		}
		final String messageAsString = msgAsString;
		topics.parallelStream().forEach(topic -> {
			ProducerRecord<String, String> record = new ProducerRecord<>(topic.getTopicName(), message.getPrimaryKey(), messageAsString);
			kafkaProducer.send(record, (metaData, expt) -> {
				if (expt != null)
				{
					logger.error("Unable to send Message.", expt);
				}
			});
		});
	}
}
