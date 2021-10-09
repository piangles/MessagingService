package org.piangles.backbone.services.msg;

class EntityProperties
{
	private static final String COMPACT = "compact";
	
	private String topicName = null;
	private String topicPurpose = null;
	private int partitionNo = 0;
	private short replicationFactor = 1;
	private Long retentionPolicy = new Long(604800000); //7 Days
	private String cleanupPolicy = COMPACT;
	private boolean readEarliest = false;
	
	
	EntityProperties(String topicName, String topicPurpose, int partitionNo, short replicationFactor, Long retentionPolicy, String cleanupPolicy, boolean readEarliest)
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


	Long getRetentionPolicy()
	{
		return retentionPolicy;
	}

	String getCleanupPolicy()
	{
		return cleanupPolicy;
	}
	
	boolean isCompacted()
	{
		return (cleanupPolicy.indexOf(COMPACT) != -1);
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
