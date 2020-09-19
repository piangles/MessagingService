package com.TBD.backbone.services.ctrl.dao;

import java.util.List;

import com.TBD.core.dao.DAOException;

public interface ControlChannelDAO
{
	public List<String> retrieveTopicsForUser(String userId) throws DAOException;
	
	public List<String> retrieveTopicsForAliases(List<String> aliases) throws DAOException;

}
