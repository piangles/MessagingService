package org.piangles.backbone.services.ctrl;

import org.piangles.core.email.EmailSupport;
import org.piangles.core.services.remoting.AbstractContainer;
import org.piangles.core.services.remoting.ContainerException;

public class ControlChannelServiceContainer extends AbstractContainer
{
	public static void main(String[] args)
	{
		ControlChannelServiceContainer container = new ControlChannelServiceContainer();
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

	public ControlChannelServiceContainer()
	{
		super("ControlChannelService");
	}
	
	@Override
	protected Object createServiceImpl() throws ContainerException
	{
		Object service = null;
		try
		{
			service = new ControlChannelServiceImpl();
		}
		catch (Exception e)
		{
			throw new ContainerException(e);
		}
		return service;
	}
}
