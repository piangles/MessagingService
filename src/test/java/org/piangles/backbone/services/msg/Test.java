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
