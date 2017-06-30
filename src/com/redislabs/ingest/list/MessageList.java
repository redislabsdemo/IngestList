package com.redislabs.ingest.list;

import redis.clients.jedis.Jedis;

/*
 * Encapsulates Redis List datastructure, provides method to push
 * messages to the left of the list, pull messages from the right
 * of the list. 
 */
public class MessageList{
	
	RedisConnection conn = null;
	Jedis jedis = null;

	// The name given to this MessageList 
	protected String name = "defaultName";
	
	/*
	 * @param name: the name given to this MessageList object 
	 */
	public MessageList(String name) throws Exception{
		this.name = name;
		conn = RedisConnection.getRedisConnection();
		jedis = conn.getJedis();
	}
	
	/*
	 * @param msg: message that's pushed to the queue
	 */
	public void push(String msg) throws Exception{
		jedis.lpush(name, msg);
	}
	
	/*
	 * @return message from the right of the queue. This is
	 * a blocking call - waits for the message if the queue
	 * is empty.
	 *  
	 */
	public String pop() throws Exception{
		// Blocking call
		return jedis.brpop(0, name).toString();
	}
	
	public Jedis getJedisInstance(){
		return jedis;
	}
}