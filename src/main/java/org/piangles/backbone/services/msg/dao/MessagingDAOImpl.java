package org.piangles.backbone.services.msg.dao;

import java.util.ArrayList;
import java.util.List;

import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.backbone.services.ctrl.Topic;
import org.piangles.core.dao.DAOException;
import org.piangles.core.dao.rdbms.AbstractDAO;
import org.piangles.core.resources.ResourceManager;

public class MessagingDAOImpl extends AbstractDAO implements MessagingDAO
{
	private static final String COMPONENT_ID = "5d435fe2-7e54-43c3-84d2-8f4addf2dac9";
	private static final String GET_TOPICS_FOR_USER_SP = "Backbone.GetTopicsForUser";
	private static final String GET_TOPICS_FOR_ALIASES_SP = "Backbone.GetTopicsForAliases";

	private static final String TOPIC = "Topic";

	public MessagingDAOImpl() throws Exception
	{
		super.init(ResourceManager.getInstance().getRDBMSDataStore(new DefaultConfigProvider("ControlChannelService", COMPONENT_ID)));
	}

	public List<Topic> retrieveTopicsForUser(String userId) throws DAOException
	{
		List<Topic> topics = new ArrayList<>();

		super.executeSPQueryProcessIndividual(GET_TOPICS_FOR_USER_SP, 1, (call) -> {
			call.setString(1, userId);
		}, (rs) -> {
			topics.add(new Topic(rs.getString(TOPIC)));
		});

		return topics;
	}

	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException
	{
		List<Topic> topics = new ArrayList<>();

		super.executeSPQueryProcessIndividual(GET_TOPICS_FOR_ALIASES_SP, 1, (call) -> {
			call.setString(1, String.join(",", aliases));
		}, (rs) -> {
			topics.add(new Topic(rs.getString(TOPIC)));
		});

		return topics;
	}
}
