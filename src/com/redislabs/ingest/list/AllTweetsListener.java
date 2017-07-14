package com.redislabs.ingest.list;

/*
 * Custom implementation of MessageListener class. This class listens to all the data down the 
 * AllData MessageList.
 * 
 */
public class AllTweetsListener extends MessageListener{
	
	private static AllTweetsListener allTweetsListener = null;
	
	/*
	 * @param name: Name of this object
	 * @param inboundListName: Name of the inbound MessageList
	 */
	public AllTweetsListener(String name, String inboundListName) throws Exception{
		super(name, inboundListName);
	}
	
	/*
	 * Factory method
	 */
	public static synchronized MessageListener getInstance() throws Exception
	{
		if(allTweetsListener == null){
			allTweetsListener = new AllTweetsListener("AllTweetsListener", "alldata");
		}

		return allTweetsListener;
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
	 * all the filters to the main listener.
	 * 
	 */
	public static void main(String[] args) throws Exception{
		MessageListener allTweetsProcessor = AllTweetsListener.getInstance();
		allTweetsProcessor.registerOutBoundMessageList(new EnglishTweetsFilter("EnglishTweetsFilter", "englishtweets"));
		allTweetsProcessor.registerOutBoundMessageList(new InfluencerFilter("InfluencerFilter", "influencers"));
		allTweetsProcessor.start();
	}
}