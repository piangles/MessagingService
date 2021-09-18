package org.piangles.backbone.services.msg;

public class TestInterpolation
{

	public static void main(String[] args)
	{
		String topic = "com.zurohq.product.users.%d";
		String entityId = "abc123";
		int bizId = 1;
		
		System.out.println(String.format(topic, bizId));

	}

}
