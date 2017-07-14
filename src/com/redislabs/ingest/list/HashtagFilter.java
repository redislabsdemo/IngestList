package com.redislabs.ingest.list;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;

/*
 * This is a custom MessageFilter class that will pull hashtags from English tweets
 * 
 */
public class HashtagFilter extends MessageFilter{

	// Regular expression for a hashtag
	Pattern HASHPATTERN = Pattern.compile("#(\\w+)");

	/*
	 * @param name: name of the filter
	 * @param listName: name of the MessageList where the messages are pushed from here
	 */
	public HashtagFilter(String name, String listName) throws Exception{
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
			System.out.println(jsonObject.get("text").getAsString());
			Matcher mat = HASHPATTERN.matcher(jsonObject.get("text").getAsString());
			
			//Repeat for each hashtag
			while(mat.find()){
				Jedis jedis = super.getJedisInstance();
				if(jedis != null){
					// Update the Sorted Set
					jedis.zincrby(super.outBoundListName, 1, mat.group(1));
				}
			}
			
		}
	}
}