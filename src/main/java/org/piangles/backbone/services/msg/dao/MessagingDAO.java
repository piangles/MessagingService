package org.piangles.backbone.services.msg.dao;

import java.util.List;

import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;

public interface MessagingDAO
{
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;

}
