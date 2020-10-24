package org.piangles.backbone.services.msg;

import static org.piangles.backbone.services.msg.Constants.SERVICE_NAME;

import org.piangles.core.email.EmailSupport;
import org.piangles.core.services.remoting.AbstractContainer;
import org.piangles.core.services.remoting.ContainerException;

public class MessagingServiceContainer extends AbstractContainer
{
	public static void main(String[] args)
	{
		MessagingServiceContainer container = new MessagingServiceContainer();
		try
		{
			container.performSteps();
		}
		catch (ContainerException e)
		{
			EmailSupport.notify(e, e.getMessage());
			System.exit(-1);
		}
	}

	public MessagingServiceContainer()
	{
		super(SERVICE_NAME);
	}
	
	@Override
	protected Object createServiceImpl() throws ContainerException
	{
		Object service = null;
		try
		{
			service = new MessagingServiceImpl();
		}
		catch (Exception e)
		{
			throw new ContainerException(e);
		}
		return service;
	}
}
