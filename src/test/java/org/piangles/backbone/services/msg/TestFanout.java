package org.piangles.backbone.services.msg;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestFanout
{
	public static void main(String[] args)
	{
		List<String> list = new ArrayList<String>();
		list.add("com.abc.def");
		List<Topic> topics = list.stream().map(topicStr -> new Topic(topicStr)).collect(Collectors.toList());
		System.out.println("TOPICS:::" + topics);
	}
}
