package org.piangles.backbone.services.msg.dao;

import java.util.List;

import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;

public interface MessagingDAO
{
	public List<Topic> retrieveTopicsForUser(String userId) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;

}
