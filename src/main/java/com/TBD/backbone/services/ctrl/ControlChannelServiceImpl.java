package com.TBD.backbone.services.ctrl;

import java.util.List;

import com.TBD.backbone.services.Locator;
import com.TBD.backbone.services.ctrl.dao.ControlChannelDAO;
import com.TBD.backbone.services.ctrl.dao.ControlChannelDAOImpl;
import com.TBD.backbone.services.logging.LoggingService;
import com.TBD.core.dao.DAOException;

public class ControlChannelServiceImpl implements ControlChannelService
{
	private LoggingService logger = Locator.getInstance().getLoggingService();
	
	private ControlChannelDAO controlChannelDAO;
	
	public ControlChannelServiceImpl() throws Exception
	{
		controlChannelDAO = new ControlChannelDAOImpl();
	}
	
	@Override
	public List<String> getTopicsFor(String userId) throws ControlChannelException
	{
		List<String> topics = null;
		logger.info("Retriving topics for user: "+userId);
		try
		{
			topics = controlChannelDAO.retrieveTopicsForUser(userId);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForUser:", e);
			throw new ControlChannelException(e);
		}
		return topics;
	}

	@Override
	public List<String> getTopicsForAliases(List<String> aliases) throws ControlChannelException
	{
		List<String> topics = null;
		logger.info("Retriving topics for aliases: "+aliases);
		try
		{
			topics = controlChannelDAO.retrieveTopicsForAliases(aliases);
		}
		catch (DAOException e)
		{
			logger.error("Failed retrieveTopicsForAliases:", e);
			throw new ControlChannelException(e);
		}
		return topics;
	}


	@Override
	public void fanOut(FanoutRequest fanoutRequest) throws ControlChannelException
	{
		// TODO Auto-generated method stub
		
	}
}
