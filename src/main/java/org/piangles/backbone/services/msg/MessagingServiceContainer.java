/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
 
 
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
			container.performSteps(args);
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
