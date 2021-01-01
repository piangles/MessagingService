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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Test
{

	public static void main(String[] args)
	{
		System.out.println(UUID.randomUUID().toString());
		
		List<String> topics = new ArrayList<String>();
		topics.add("Tech");
		topics.add("Energy");
		System.out.println(String.join(",", topics));
		
		int totalCountOfRecordsPerPartition = 35;
		int offset = 10;
		Map<String, Integer> userIdMap = new LinkedHashMap<>();
		for (int i=1; i <= totalCountOfRecordsPerPartition; ++i)
		{
			userIdMap.put("UserId:" + i, i + offset);
		}
		
//		userIdMap.clear();
//		userIdMap.put("UserId:1", 1);
//		userIdMap.put("UserId:33", 33);
		
		Set<String> keys = userIdMap.keySet();
		for (String key : keys)
		{
			String userId = key;
			Integer userNoInt = userIdMap.get(userId);
			if (userNoInt != null)
			{
				int userNo = userNoInt.intValue();
				int partitionNo = userNo % (totalCountOfRecordsPerPartition + 1);
				System.out.println(userId + " : " + partitionNo);
			}
		}
	}

}
