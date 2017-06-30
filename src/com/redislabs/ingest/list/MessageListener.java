package com.redislabs.ingest.list;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 * A MessageListener pulls messages from a queue. It registers n number of 
 * MessageFilter objects. MessageLister pushes a copy of the message to 
 * each of the MessageFilter objects.
 * 
 * Extend MessageListener class to implement your own class. Override processMessage()
 * method if required.
 * 	
 */
class MessageListener implements Runnable{
	// Name of this object
	private String name = null;
	
	// MessageList object that supplies messages
	private MessageList inboundList = null;
	
	// MessageFilter objects that receive messages from this listener
	Map<String, MessageFilter> outBoundMsgFilters = new HashMap<String, MessageFilter>();
	
	/*
	 * @param name: Name of this MessageListener
	 * @param inboundListName: Name of the MessageList that supplies messages
	 */
	public MessageListener(String name, String inboundListName) throws Exception{
		this.name = name;
		this.inboundList = new MessageList(inboundListName);
	}
	
	/*
	 * @param msgFilter: MessageFilter object that's registered to receive messages
	 */
	public void registerOutBoundMessageList(MessageFilter msgFilter){
		if(msgFilter != null){
			if(outBoundMsgFilters.get(msgFilter.name) == null){
				outBoundMsgFilters.put(msgFilter.name, msgFilter);
			}
			
		}
	}
	
	/*
	 * Start the thread. 
	 */
	public void start() throws Exception{
		Thread t = new Thread(this);
		t.start();		
	}
	
	@Override
	public void run(){
		try{
			System.out.println("Starting Processor "+name);
			while(true){
				String msg = inboundList.pop();
				processMessage(msg);
				
			}					
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/*
	 * Process message before pushing it to the MessageFilter objects.
	 * Override this method in the child class if required.
	 */
	protected void processMessage(String msg) throws Exception{
		pushMessage(msg);
	}
	
	/*
	 * Push the message to each of the registered MessageFilter objects.
	 */
	protected void pushMessage(String msg) throws Exception{
		Set<String> outBoundMsgNames = outBoundMsgFilters.keySet();
		
		for(String name : outBoundMsgNames ){
			MessageFilter msgList = outBoundMsgFilters.get(name);
			msgList.filterAndPush(msg);
		}
	}
}
