package com.redislabs.ingest.list;

import java.util.HashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;

/*
 * This is a custom MessageFilter class that will enable filtering 
 * tweets from users with more than 10000 followers.
 * 
 */
public class InfluencerFilter extends MessageFilter{

	/*
	 * @param name: name of the filter
	 * @param listName: name of the MessageList where the messages are pushed from here
	 */
	public InfluencerFilter(String name, String listName) throws Exception{
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
		
		JsonObject userObject = jsonObject.get("user").getAsJsonObject();
		
		JsonElement followerCountElm = userObject.get("followers_count");
		try{
			if(followerCountElm != null && followerCountElm.getAsDouble() > 10000){
				String name = userObject.get("name").getAsString();
				String screenName = userObject.get("screen_name").getAsString();
				int followerCount = userObject.get("followers_count").getAsInt();
				int friendCount = userObject.get("friends_count").getAsInt();
							
				HashMap<String, String> map = new HashMap<String, String>();
				map.put("name", name);
				map.put("screen_name", screenName);
				if(userObject.get("location") != null){
					map.put("location", userObject.get("location").getAsString());				
				}
				map.put("followers_count", Integer.toString(followerCount));
				map.put("friendCount", Integer.toString(friendCount));
				
				Jedis jedis = super.getJedisInstance();
				if(jedis != null){
					// Update the Sorted Set
					jedis.zadd("Influencers", followerCount, screenName);
					
					// Update the Hash 
					jedis.hmset("Influencer:"+screenName, map);
					
					//System.out.println(userObject.get("screen_name").getAsString()+"| Followers:"+userObject.get("followers_count").getAsString());								
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}