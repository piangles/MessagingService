package org.piangles.backbone.services.msg.dao;

import java.util.Arrays;
import java.util.List;

import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;
import org.piangles.core.dao.rdbms.AbstractDAO;
import org.piangles.core.resources.ResourceManager;

public class MessagingDAOImpl extends AbstractDAO implements MessagingDAO
{
	private static final String COMPONENT_ID = "5d435fe2-7e54-43c3-84d2-8f4addf2dac9";
	private static final String GET_TOPICS_FOR_ENTITIES_SP = "Backbone.GetTopicsForEntities";
	private static final String GET_TOPICS_FOR_ALIASES_SP = "Backbone.GetTopicsForAliases";

	private static final String TOPIC = "Topic";
	private static final String PARTITION = "PartitionNo";

	public MessagingDAOImpl() throws Exception
	{
		super.init(ResourceManager.getInstance().getRDBMSDataStore(new DefaultConfigProvider("MessagingService", COMPONENT_ID)));
	}

	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ALIASES_SP, 1, (call) -> {
			call.setString(1, String.join(",", aliases));
		}, (rs) -> {
			return new Topic(rs.getString(TOPIC), rs.getInt(PARTITION));
		});

		return topics;
	}
	
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException
	{
		return retrieveTopicsForEntities(entityType, Arrays.asList(entityId));
	}
	
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ENTITIES_SP, 2, (call) -> {
			call.setString(1, entityType);
			call.setString(2, String.join(",", entityIds));
		}, (rs) -> {
			return new Topic(rs.getString(TOPIC), rs.getInt(PARTITION));
		});

		return topics;
	}
}
