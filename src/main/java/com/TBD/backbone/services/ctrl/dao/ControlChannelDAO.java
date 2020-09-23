package com.TBD.backbone.services.ctrl.dao;

import java.util.List;

import com.TBD.backbone.services.ctrl.Topic;
import com.TBD.core.dao.DAOException;

public interface ControlChannelDAO
{
	public List<Topic> retrieveTopicsForUser(String userId) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;

}
