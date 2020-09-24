package org.piangles.backbone.services.ctrl.dao;

import java.util.ArrayList;
import java.util.List;

import org.piangles.core.dao.DAOException;
import org.piangles.core.dao.rdbms.AbstractDAO;
import org.piangles.core.resources.ResourceManager;

import com.TBD.backbone.services.config.DefaultConfigProvider;

public class ControlChannelDAOImpl extends AbstractDAO implements ControlChannelDAO
{
	private static final String COMPONENT_ID = "5d435fe2-7e54-43c3-84d2-8f4addf2dac9";
	private static final String GET_TOPICS_FOR_USER_SP = "Backbone.GetTopicsForUser";
	private static final String GET_TOPICS_FOR_ALIASES_SP = "Backbone.GetTopicsForAliases";

	private static final String TOPIC = "Topic";

	public ControlChannelDAOImpl() throws Exception
	{
		super.init(ResourceManager.getInstance().getRDBMSDataStore(new DefaultConfigProvider("ControlChannelService", COMPONENT_ID)));
	}

	public List<String> retrieveTopicsForUser(String userId) throws DAOException
	{
		List<String> topics = new ArrayList<String>();

		super.executeSPQueryProcessIndividual(GET_TOPICS_FOR_USER_SP, 1, (call) -> {
			call.setString(1, userId);
		}, (rs) -> {
			topics.add(rs.getString(TOPIC));
		});

		return topics;
	}

	public List<String> retrieveTopicsForAliases(List<String> aliases) throws DAOException
	{
		List<String> topics = new ArrayList<String>();

		super.executeSPQueryProcessIndividual(GET_TOPICS_FOR_ALIASES_SP, 1, (call) -> {
			call.setString(1, String.join(",", aliases));
		}, (rs) -> {
			topics.add(rs.getString(TOPIC));
		});

		return topics;
	}
}
