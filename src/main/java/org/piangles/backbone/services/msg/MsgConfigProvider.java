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
