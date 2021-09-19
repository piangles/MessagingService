package org.piangles.backbone.services.msg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class EntityConfiguration
{
	private static final String ENTITY_TYPE = "Entity%d.Type";
	private static final String ENTITY_TOPIC_NAME = "Entity%d.Topic.Name";
	private static final String ENTITY_TOPIC_PURPOSE = "Entity%d.Topic.Purpose";
	private static final String ENTITY_TOPIC_PARTITIONS = "Entity%d.Topic.Partitions";
	private static final String ENTITY_TOPIC_REPLICATION_FACTOR = "Entity%d.Topic.ReplicationFactor";
	private static final String ENTITY_TOPIC_RETENTION_POLICY = "Entity%d.Topic.RetentionPolicy";
	private static final String ENTITY_TOPIC_CLEANUP_POLICY = "Entity%d.Topic.CleanupPolicy";
	private static final String ENTITY_TOPIC_READ_EARLIEST = "Entity%d.Topic.ReadEarliest";
	

	private Map<String, List<EntityProperties>> entityPropertyMap = null;
	
	EntityConfiguration(Properties props)
	{
		entityPropertyMap = new HashMap<>();
		
		String entityType = null;
		String topicName = null;
		String topicPurpose = null;
		int noOfPartitions = 1;
		short replicationFactor = 1;
		long retentionPolicy = 0;
		String cleanupPolicy = "compact";
		boolean readEarliest = false;

		int count = 0;
		List<EntityProperties> entityProperties = null;
		while (true)
		{
			entityType = props.getProperty(String.format(ENTITY_TYPE, count));
			
			if (entityType != null)
			{
				entityProperties = entityPropertyMap.get(entityType);
				if (entityProperties == null)
				{
					entityProperties = new ArrayList<EntityProperties>();
					entityPropertyMap.put(entityType, entityProperties);
				}
				
				topicName = props.getProperty(String.format(ENTITY_TOPIC_NAME, count));
				topicPurpose = props.getProperty(String.format(ENTITY_TOPIC_PURPOSE, count));
				noOfPartitions = Integer.parseInt(props.getProperty(String.format(ENTITY_TOPIC_PARTITIONS, count)));
				replicationFactor = Short.parseShort(props.getProperty(String.format(ENTITY_TOPIC_REPLICATION_FACTOR, count)));
				retentionPolicy = Long.parseLong(props.getProperty(String.format(ENTITY_TOPIC_RETENTION_POLICY, count)));
				cleanupPolicy = props.getProperty(String.format(ENTITY_TOPIC_CLEANUP_POLICY, count));
				readEarliest = Boolean.parseBoolean(props.getProperty(String.format(ENTITY_TOPIC_READ_EARLIEST, count)));
				
				entityProperties.add(new EntityProperties(topicName, topicPurpose, noOfPartitions, replicationFactor, retentionPolicy, cleanupPolicy, readEarliest));
			}
			else
			{
				break;
			}
			count++;
		}
	}
	
	public List<EntityProperties> getEntityProperties(String entityType)
	{
		return entityPropertyMap.get(entityType);
	}
}
