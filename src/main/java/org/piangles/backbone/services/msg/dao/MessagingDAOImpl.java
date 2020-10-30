package org.piangles.backbone.services.msg.dao;

import static org.piangles.backbone.services.msg.Constants.PARTITION_ALGORITHM;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.backbone.services.msg.PartitionerAlgorithm;
import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;
import org.piangles.core.dao.rdbms.AbstractDAO;
import org.piangles.core.resources.ResourceManager;

public class MessagingDAOImpl extends AbstractDAO implements MessagingDAO
{
	private static final String COMPONENT_ID = "5d435fe2-7e54-43c3-84d2-8f4addf2dac9";
	private static final String GET_PARTITION_ALGORITHM_FOR_TOPICS_SP = "Backbone.GetPartitionerAlgorithmForTopics";
	private static final String GET_TOPICS_FOR_ENTITIES_SP = "Backbone.GetTopicsForEntities";
	private static final String GET_TOPICS_FOR_ALIASES_SP = "Backbone.GetTopicsForAliases";

	private static final String TOPIC = "Topic";
	private static final String PARTITION = "PartitionNo";
	private static final String PARTITIONER_ALGO = "PartitionerAlgorithm";

	public MessagingDAOImpl() throws Exception
	{
		super.init(ResourceManager.getInstance().getRDBMSDataStore(new DefaultConfigProvider("MessagingService", COMPONENT_ID)));
	}
	
	@Override
	public Map<String, PartitionerAlgorithm> retrievePartitionerAlgorithmForTopics() throws DAOException
	{
		Map<String, PartitionerAlgorithm> topicAlgoMap = new HashMap<>();
		super.executeSPQueryList(GET_PARTITION_ALGORITHM_FOR_TOPICS_SP, (rs, call)->{
			String topic = rs.getString(TOPIC);
			String algorithm = rs.getString(PARTITIONER_ALGO);
			topicAlgoMap.put(PARTITION_ALGORITHM + topic, PartitionerAlgorithm.valueOf(algorithm));
			return null;
		});
		return topicAlgoMap;
	}

	@Override
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ALIASES_SP, 1, (call) -> {
			call.setString(1, String.join(",", aliases));
		}, (rs, call) -> {
			return new Topic(rs.getString(TOPIC), rs.getInt(PARTITION));
		});

		return topics;
	}
	
	@Override
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException
	{
		return retrieveTopicsForEntities(entityType, Arrays.asList(entityId));
	}
	
	@Override
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ENTITIES_SP, 2, (call) -> {
			call.setString(1, entityType);
			call.setString(2, String.join(",", entityIds));
		}, (rs, call) -> {
			return new Topic(rs.getString(TOPIC), rs.getInt(PARTITION));
		});

		return topics;
	}
}
