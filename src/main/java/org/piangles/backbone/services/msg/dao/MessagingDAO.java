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

import java.util.List;
import java.util.Map;

import org.piangles.backbone.services.msg.PartitionerAlgorithm;
import org.piangles.backbone.services.msg.Topic;
import org.piangles.core.dao.DAOException;

public interface MessagingDAO
{
	public Topic retrieveTopic(String topicName) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntity(String entityType, String entityId) throws DAOException;
	
	public List<Topic> retrieveTopicsForEntities(String entityType, List<String> entityIds) throws DAOException;
	
	public List<Topic> retrieveTopicsForAliases(List<String> aliases) throws DAOException;
	
	public Map<String, PartitionerAlgorithm> retrievePartitionerAlgorithmForTopics() throws DAOException;
}
