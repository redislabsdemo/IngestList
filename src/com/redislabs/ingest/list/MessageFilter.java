package com.redislabs.ingest.list;

import redis.clients.jedis.Jedis;

/*
 * Master class that provides APIs to filter messages.
 * A MessageFilter receives messages from a MessageListener. It 
 * then filters the messages as required, and pushes the messages
 * to the message queue of the next level.
 * 
 * Extend this class to implement your own custom filter.
 */
public class MessageFilter{
	
	// Name of this filter
	protected String name = null;
	
	// The MessageList where the data is pushed down this filter
	MessageList messageList = null;
	
	// Name of the outbound list 
	protected String outBoundListName = null;
	
	/*
	 * @param name: Name of this MessageFilter object
	 * @param listName: Name of the outbound MessageList
	 */
	public MessageFilter(String name, String listName) throws Exception{
		this.name = name;
		this.outBoundListName = listName; 
		this.messageList = new MessageList(listName);
	}

	/*
	 * Override this method in your implementation.
	 */
	public void filterAndPush(String msg) throws Exception{
		messageList.push(msg);
	}
	
	public Jedis getJedisInstance(){
		Jedis jedis = null;
		if(messageList != null){
			jedis = messageList.getJedisInstance();
		}
		
		return jedis;
	}
	
}