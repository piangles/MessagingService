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

	private MessagingDAO messagingDAO = null;
	private KafkaProducer<String, String> kafkaProducer = null;

	public MessagingServiceImpl() throws Exception
	{
		messagingDAO = new MessagingDAOImpl();
		KafkaMessagingSystem kms = ResourceManager.getInstance().getKafkaMessagingSystem(new DefaultConfigProvider("MessagingService", COMPONENT_ID));
		kafkaProducer = kms.createProducer();

		Message message = new Message("1", new ControlDetails("Hello World", Action.Add, "This is the content"));
		FanoutRequest fanoutRequest = new FanoutRequest(EntityIdType.Topic, message);
		fanoutRequest.getEntityIds().add("com.TBD.playground");
		fanOut(fanoutRequest);
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
			topics = messagingDAO.retrieveTopicsForUser(userId);
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

	/**
	 * Distribute the message on all the topics listed.
	 * 
	 */
	@Override
	public void fanOut(FanoutRequest fanoutRequest) throws MessagingException
	{
		List<Topic> topics = null;
		switch (fanoutRequest.getDistributionListType())
		{
		case Alias:
			topics = getTopicsForAliases(fanoutRequest.getDistributionList());
		case Topic:
			topics = fanoutRequest.getDistributionList().stream().map(topicStr -> new Topic(topicStr)).collect(Collectors.toList());
		case Entity:
			//1 - using the list of entities get all the topics
			//2 - 
			topics = null;
		default:
			topics = null;
			logger.info("Invalid");
			return;
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
				
				String actualKey = message.getPrimaryKey();//Sent by from the caller
				String mappedKey = null; //Mapped PartitionNo from the Table
				
				ProducerRecord<String, String> record = new ProducerRecord<>(
															topic.getTopicName(), 
															mappedPrimaryKey,
															messageAsString);
				
				kafkaProducer.send(record, (metaData, expt) -> {
					if (expt != null)
					{
						logger.error("Unable to send Message.", expt);
					}
					else
					{
						System.out.println(">>>>>>>>>>>>>>>" + metaData.partition());
					}
			});
		});
	}
}
