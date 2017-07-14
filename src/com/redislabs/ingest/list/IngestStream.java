package com.redislabs.ingest.list;

import java.util.Arrays;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

/*
 * This is the main class - the starting point of the ingest process. 
 * Listens to the Twitter feed via PubNub and pushes all the messages 
 * to the main List data structure that queues them up. 
 * 
 */
public class IngestStream
{
	final static String SUB_KEY_TWITTER = "sub-c-78806dd4-42a6-11e4-aed8-02ee2ddab7fe";
	final static String CHANNEL_TWITTER = "pubnub-twitter";
	
	
	// The common MessageList that queues all messages
	private MessageList allMsgs = null;
		
	/*
	 * Start the main program; start the Twitter listener 
	 */
	public static void main(String[] args) throws Exception
	{
		IngestStream ing = new IngestStream();
		try{
			ing.start();	
		}catch(Exception pe){
			pe.printStackTrace();
		}
	}
	
	/*
	 * Startup program
	 */
	public void start() throws Exception{
		
		// PubNub listener
		PNConfiguration pnConfig = new PNConfiguration();
		pnConfig.setSubscribeKey(SUB_KEY_TWITTER);
		pnConfig.setSecure(false);
		PubNub pubnub = new PubNub(pnConfig);
		pubnub.subscribe().channels(Arrays.asList(CHANNEL_TWITTER)).execute();
		
		// The list that queues up all the messages
		allMsgs = new MessageList("alldata");		
		
		// PubNub callback that's passed to the listener
		SubscribeCallback subscribeCallback = new SubscribeCallback() {
		    @Override
		    public void status(PubNub pubnub, PNStatus status) {
		        if (status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory) {
		            // internet got lost, do some magic and call reconnect when ready
		            pubnub.reconnect();
		        } else if (status.getCategory() == PNStatusCategory.PNTimeoutCategory) {
		            // do some magic and call reconnect when ready
		            pubnub.reconnect();
		        } else {
		            System.out.println(status.toString());
		        }
		    }
		 
		    @Override
		    public void message(PubNub pubnub, PNMessageResult message) {
		    	try{
		    		// Queue up all the messages in the list
		    		System.out.println(message.getMessage().getAsJsonObject().toString());
			    	allMsgs.push(message.getMessage().getAsJsonObject().toString());		    		
		    	}catch(Exception e){
		    		e.printStackTrace();
		    	}
		    }
		 
		    @Override
		    public void presence(PubNub pubnub, PNPresenceEventResult presence) {
		 
		    }
		};
		 
		// Add the listener
		pubnub.addListener(subscribeCallback);	
	}
}