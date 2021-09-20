package org.piangles.backbone.services.msg;

class EntityProperties
{
	private String topicName = null;
	private String topicPurpose = null;
	private int partitionNo = 0;
	private short replicationFactor = 1;
	private long retentionPolicy = 604800000;
	private String cleanupPolicy = "compact";
	private boolean readEarliest = false;
	
	
	EntityProperties(String topicName, String topicPurpose, int partitionNo, short replicationFactor, long retentionPolicy, String cleanupPolicy, boolean readEarliest)
	{
		this.topicName = topicName;
		this.topicPurpose = topicPurpose;
		this.partitionNo = partitionNo;
		this.replicationFactor = replicationFactor;
		this.retentionPolicy = retentionPolicy;
		this.cleanupPolicy = cleanupPolicy;
		this.readEarliest = readEarliest;
	}


	String getTopicName()
	{
		return topicName;
	}

	String getTopicPurpose()
	{
		return topicPurpose;
	}


	int getPartitionNo()
	{
		return partitionNo;
	}


	short getReplicationFactor()
	{
		return replicationFactor;
	}


	long getRetentionPolicy()
	{
		return retentionPolicy;
	}

	String getCleanupPolicy()
	{
		return cleanupPolicy;
	}

	boolean shouldReadEarliest()
	{
		return readEarliest;
	}

	@Override
	public String toString()
	{
		return "EntityProperties [topicName=" + topicName + ", topicPurpose=" + topicPurpose + ", partitionNo=" + partitionNo + ", replicationFactor=" + replicationFactor + ", retentionPolicy="
				+ retentionPolicy + ", cleanupPolicy=" + cleanupPolicy + ", readEarliest=" + readEarliest + "]";
	}
}
