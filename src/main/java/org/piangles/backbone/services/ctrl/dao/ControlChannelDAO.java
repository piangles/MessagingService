package org.piangles.backbone.services.ctrl.dao;

import java.util.List;

import org.piangles.backbone.services.ctrl.Topic;
import org.piangles.core.dao.DAOException;

public interface ControlChannelDAO
{
	public List<Topic> retrieveTopicsForUser(String userId) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;

}
