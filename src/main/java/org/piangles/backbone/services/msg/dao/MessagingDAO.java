package org.piangles.backbone.services.msg.dao;

import java.util.List;
import java.util.Map;

import org.piangles.backbone.services.msg.PartitionerAlgorithm;
import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;

public interface MessagingDAO
{
	public Topic retrieveTopic(String topicName) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;
	
	public Map<String, PartitionerAlgorithm> retrievePartitionerAlgorithmForTopics() throws DAOException;
}
