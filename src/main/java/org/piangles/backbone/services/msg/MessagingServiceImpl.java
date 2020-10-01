package org.piangles.backbone.services.msg;

import java.util.List;

import org.piangles.backbone.services.ctrl.ControlChannelException;
import org.piangles.backbone.services.ctrl.ControlChannelService;
import org.piangles.backbone.services.ctrl.FanoutRequest;
import org.piangles.backbone.services.ctrl.Topic;
import org.piangles.core.dao.DAOException;

import org.piangles.backbone.services.Locator;
import org.piangles.backbone.services.logging.LoggingService;
import org.piangles.backbone.services.msg.dao.MessagingDAO;
import org.piangles.backbone.services.msg.dao.MessagingDAOImpl;

public class MessagingServiceImpl implements ControlChannelService
{
	private LoggingService logger = Locator.getInstance().getLoggingService();
	
	private MessagingDAO controlChannelDAO;
	
	public MessagingServiceImpl() throws Exception
	{
		controlChannelDAO = new MessagingDAOImpl();
	}
	
	/**
	 * All topics here are to be log compacted
	 */
	@Override
	public List<Topic> getTopicsFor(String userId) throws ControlChannelException
	{
		List<Topic> topics = null;
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

	/**
	 * Should all topics here be log compacted????
	 */
	@Override
	public List<Topic> getTopicsForAliases(List<String> aliases) throws ControlChannelException
	{
		List<Topic> topics = null;
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


	/**
	 * The only time fanOut is to be called is when changes to Key element(s)
	 * of an Entity result in either and Subscription or Unsubcription.
	 * 
	 * 	// Users can delete messages entirely by writing a so-called tombstone message with null-value for a specific key.

	 */
	@Override
	public void fanOut(FanoutRequest fanoutRequest) throws ControlChannelException
	{
		//Convert FanoutRequest to ControlMessage
	}
}
