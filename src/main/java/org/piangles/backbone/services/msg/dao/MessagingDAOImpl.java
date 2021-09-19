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
 
 
 
package org.piangles.backbone.services.msg.dao;

import static org.piangles.backbone.services.msg.Constants.PARTITION_ALGORITHM;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.piangles.backbone.services.config.DefaultConfigProvider;
import org.piangles.backbone.services.msg.MessagingService;
import org.piangles.backbone.services.msg.PartitionerAlgorithm;
import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;
import org.piangles.core.dao.rdbms.AbstractDAO;
import org.piangles.core.resources.ResourceManager;

public class MessagingDAOImpl extends AbstractDAO implements MessagingDAO
{
	private static final String COMPONENT_ID = "5d435fe2-7e54-43c3-84d2-8f4addf2dac9";
	
	private static final String SAVE_TOPIC_ENTITY_SP = "msg.save_topic_for_entry";
	private static final String GET_TOPIC_DETAILS_SP = "msg.get_topic_details";
	private static final String GET_TOPICS_FOR_ENTITIES_SP = "msg.get_topics_for_entities";
	private static final String GET_TOPICS_FOR_ALIASES_SP = "msg.get_topics_for_aliases";
	private static final String GET_PARTITION_ALGORITHM_FOR_TOPICS_SP = "msg.get_partitioner_algorithm_for_topics";

	private static final int DEFAULT_PARTITION = 0;
	private static final String TOPIC = "topic";
	private static final String PURPOSE = "purpose";
	private static final String PARTITION = "partition_no";
	private static final String PARTITIONER_ALGO = "partitioner_algorithm";
	private static final String COMPACTED = "compacted";
	private static final String READ_EARLIEST = "read_earliest";

	public MessagingDAOImpl() throws Exception
	{
		super.init(ResourceManager.getInstance().getRDBMSDataStore(new DefaultConfigProvider(MessagingService.NAME, COMPONENT_ID)));
	}

	@Override
	public void saveTopicsForEntity(String entityType, String entityId, Topic topic) throws DAOException
	{
		super.executeSP(SAVE_TOPIC_ENTITY_SP, 7, (call)->{
			
			call.setString(1, entityType);
			call.setString(2, entityId);
			call.setString(3, topic.getTopicName());
			call.setString(4, topic.getPurpose());
			call.setInt(5, topic.getPartition());
			call.setBoolean(6, topic.isCompacted());
			call.setBoolean(7, topic.shouldReadEarliest());
			
		});

	}
	
	@Override
	public Topic retrieveTopic(String topicName) throws DAOException
	{
		Topic topic = null;
		topic = super.executeSPQuery(GET_TOPIC_DETAILS_SP, 1, (call)->{call.setString(1, topicName);}, (rs, call)->{
			Topic dbTopic = null;
			String algorithm = rs.getString(PARTITIONER_ALGO);
			if (PartitionerAlgorithm.valueOf(algorithm) == PartitionerAlgorithm.Default)
			{
				dbTopic = new Topic(rs.getString(TOPIC), DEFAULT_PARTITION, rs.getBoolean(READ_EARLIEST)); 
			}
			else
			{
				/**
				 * The caller has to determine the logic just as the sender on what 
				 * Parition is going to be used as CustomPartition will kick in. 
				 */
				dbTopic = new Topic(rs.getString(TOPIC), Topic.CUSTOM_PARTIONED, rs.getBoolean(READ_EARLIEST)); 
			}
			return dbTopic;
		});
		return topic;
	}

	@Override
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ALIASES_SP, 1, (call) -> {
			call.setString(1, String.join(",", aliases));
		}, (rs, call) -> {
			return new Topic(rs.getString(TOPIC), rs.getInt(PARTITION), rs.getBoolean(READ_EARLIEST));
		});

		return topics;
	}
	
	@Override
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException
	{
		return retrieveTopicsForEntities(entityType, Arrays.asList(entityId));
	}
	
	@Override
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException
	{
		List<Topic> topics = super.executeSPQueryList(GET_TOPICS_FOR_ENTITIES_SP, 2, (call) -> {
			call.setString(1, entityType);
			call.setString(2, String.join(",", entityIds));
		}, (rs, call) -> {
			return new Topic(rs.getString(TOPIC), rs.getString(PURPOSE), rs.getInt(PARTITION), rs.getBoolean(COMPACTED), rs.getBoolean(READ_EARLIEST));
		});

		return topics;
	}
	
	@Override
	public Map<String, PartitionerAlgorithm> retrievePartitionerAlgorithmForTopics() throws DAOException
	{
		Map<String, PartitionerAlgorithm> topicAlgoMap = new HashMap<>();
		super.executeSPQueryList(GET_PARTITION_ALGORITHM_FOR_TOPICS_SP, (rs, call)->{
			String topic = rs.getString(TOPIC);
			String algorithm = rs.getString(PARTITIONER_ALGO);
			topicAlgoMap.put(PARTITION_ALGORITHM + topic, PartitionerAlgorithm.valueOf(algorithm));
			return null;
		});
		return topicAlgoMap;
	}
}
