package com.TBD.backbone.services.ctrl;

import java.util.ArrayList;
import java.util.List;
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
	}

}
