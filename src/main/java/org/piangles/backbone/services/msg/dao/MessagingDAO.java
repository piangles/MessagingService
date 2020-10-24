package org.piangles.backbone.services.msg.dao;

import java.util.List;
import java.util.Map;

import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;

public interface MessagingDAO
{
	public Map<String, String> retrievePartitionerAlgorithmForTopics() throws DAOException;
	
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;
}
