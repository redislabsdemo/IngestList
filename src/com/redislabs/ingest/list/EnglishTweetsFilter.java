package com.redislabs.ingest.list;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;


/*
 * This is a custom MessageFilter class that will enable filtering 
 * tweets that are marked lang=en. (English tweets)
 * 
 */
public class EnglishTweetsFilter extends MessageFilter{
	
	/*
	 * @param name: name of the filter
	 * @param listName: name of the MessageList where the messages are pushed from here
	 */
	public EnglishTweetsFilter(String name, String listName) throws Exception{
		super(name, listName);
	}
	
	/*
	 * Custom implementation of MessageFilter
	 * @see com.redislabs.ingest.list.MessageFilter#filterAndPush(java.lang.String)
	 */
	@Override
	public void filterAndPush(String message) throws Exception{
		JsonParser jsonParser = new JsonParser();
		
		JsonElement jsonElement = jsonParser.parse(message);
		JsonArray jsonArray = jsonElement.getAsJsonArray();
		JsonObject jsonObject = jsonArray.get(1).getAsJsonObject();
		if(jsonObject.get("lang") != null && jsonObject.get("lang").getAsString().equals("en")){
			
			Jedis jedis = super.getJedisInstance();
			if(jedis != null){
				jedis.lpush(super.outBoundListName, jsonObject.toString());				
			}
		}
	}
}