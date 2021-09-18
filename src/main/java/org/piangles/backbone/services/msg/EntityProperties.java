package org.piangles.backbone.services.msg;

class EntityProperties
{
	private String topicName = null;
	private String topicPurpose = null;
	private int noOfPartitions = 1;
	private short replicationFactor = 1;
	private String cleanupPolicy = "compact";
	private long retentionPolicy = 604800000;
	
	
	EntityProperties(String topicName, String topicPurpose, int noOfPartitions, short replicationFactor, String cleanupPolicy, long retentionPolicy)
	{
		this.topicName = topicName;
		this.topicPurpose = topicPurpose;
		this.noOfPartitions = noOfPartitions;
		this.replicationFactor = replicationFactor;
		this.cleanupPolicy = cleanupPolicy;
		this.retentionPolicy = retentionPolicy;
	}


	public String getTopicName()
	{
		return topicName;
	}


	public String getTopicPurpose()
	{
		return topicPurpose;
	}


	public int getNoOfPartitions()
	{
		return noOfPartitions;
	}


	public short getReplicationFactor()
	{
		return replicationFactor;
	}


	public String getCleanupPolicy()
	{
		return cleanupPolicy;
	}


	public long getRetentionPolicy()
	{
		return retentionPolicy;
	}


	@Override
	public String toString()
	{
		return "EntityProperties [topicName=" + topicName + ", topicPurpose=" + topicPurpose + ", noOfPartitions=" + noOfPartitions + ", replicationFactor=" + replicationFactor + ", cleanupPolicy="
				+ cleanupPolicy + ", retentionPolicy=" + retentionPolicy + "]";
	}

	
}
