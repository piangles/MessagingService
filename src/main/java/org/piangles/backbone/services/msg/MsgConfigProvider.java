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

import java.util.Map;
import java.util.Properties;

import org.piangles.backbone.services.Locator;
import org.piangles.core.util.abstractions.AbstractConfigProvider;

public class MsgConfigProvider extends AbstractConfigProvider
{
	private static final String COMPONENT_ID = "fd5f51bc-5a14-4675-9df4-982808bb106b";
	private Map<String, PartitionerAlgorithm> partitionerAlgorithmForTopics = null;

	public MsgConfigProvider(Map<String, PartitionerAlgorithm> partitionerAlgorithmForTopics)
	{
		super(SERVICE_NAME, COMPONENT_ID);
		this.partitionerAlgorithmForTopics = partitionerAlgorithmForTopics;
	}

	@Override
	public Properties getProperties() throws Exception
	{
		Properties props = Locator.getInstance().getConfigService().getConfiguration(getComponentId()).getProperties();
		partitionerAlgorithmForTopics.keySet().stream().forEach(key -> {
			props.put(key, partitionerAlgorithmForTopics.get(key));
		});
		return props;
	}
}
