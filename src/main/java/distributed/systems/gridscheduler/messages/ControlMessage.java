package distributed.systems.gridscheduler.messages;

import distributed.systems.core.Message;

/**
 * 
 * Class that represents the messages being exchanged in the VGS. It has some members to
 * facilitate the passing of common arguments. Feel free to expand it and adapt it to your 
 * needs. 
 * 
 * @author Niels Brouwers
 * @author Dorian de Koning
 *
 */
public abstract class ControlMessage implements Message {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = -1453428681740343634L;


	protected String senderName;
	private long sequenceID;

	/**
	 * Constructs a new ControlMessage object
	 * @param senderName name of the sender
	 * @param sequenceId sequenceID of the message
	 */
	public ControlMessage(String senderName) {
		this.senderName = senderName;
	}

	/**
	 * @return the name of the sender
	 */
	public String getSenderName() {
		return senderName;
	}

	/**
	 * Set sequence id
	 * @param sequenceId the new sequenceid
	 */
	public void setSequenceID(long sequenceID){
		this.sequenceID = sequenceID;
	}

	/**
	 * @return the sequenceid of this message
	 */
	public long getSequenceID() {
		return this.sequenceID;
	}

	public static class ResourceManagerJoinMessage extends ControlMessage{
		private static final long serialVersionUID = -1453428681740343634L;
		private int nodeCount;
		public ResourceManagerJoinMessage(String senderName, int nodeCount){
			super(senderName);
			this.nodeCount = nodeCount;
		}

		public int getNodeCount(){
			return nodeCount;
		}
	}

	public static class RequestLoadMessage extends ControlMessage{
		private static final long serialVersionUID = -1453428681740343634L;
		public RequestLoadMessage(String senderName){
			super(senderName);
		}
	}

}
