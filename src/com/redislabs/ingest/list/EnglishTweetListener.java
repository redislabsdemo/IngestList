package com.redislabs.ingest.list;

/*
 * Custom implementation of MessageListener class. This class listens to all English Tweets
 * 
 */
public class EnglishTweetListener extends MessageListener{
	
	private static EnglishTweetListener englishTweetListener = null;
	
	/*
	 * @param name: Name of this object
	 * @param inboundListName: Name of the inbound MessageList
	 */
	public EnglishTweetListener(String name, String inboundListName) throws Exception{
		super(name, inboundListName);
	}

	/*
	 * Factory method
	 */
	public static synchronized MessageListener getInstance() throws Exception
	{
		if(englishTweetListener == null){
			englishTweetListener = new EnglishTweetListener("EnglishTweetListener", "englishtweets");
		}

		return englishTweetListener;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.redislabs.ingest.list.MessageListener#processMessage(java.lang.String)
	 */
	@Override
	protected void processMessage(String msg) {
		try{			
			super.pushMessage(msg);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/*
	 * This is the main method that starts this listener. The main method also binds
	 * all the filters to the listener.
	 * 
	 */
	public static void main(String[] args) throws Exception{
		MessageListener englishTweetListener = EnglishTweetListener.getInstance();
		englishTweetListener.registerOutBoundMessageList(new HashtagFilter("HashtagFilter", "hashtagset"));
		englishTweetListener.start();
	}
}