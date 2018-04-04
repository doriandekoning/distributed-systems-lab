package distributed.systems.gridscheduler.messages;

/**
 * 
 * Class that represents an acknowledgement for an ControlMessage
 * 
 * @author Dorian de Koning
 *
 */
public class AcknowledgementMessage extends ControlMessage {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = -1453428681740343634L;

	private long jobID;
	private ControlMessage message;

	/**
	 * Constructs a new ControlMessage object
	 * @param type the type of control message
	 */
	public AcknowledgementMessage(String name, ControlMessage message) {
		super(name);
		this.message = message;
	}
	
	public void setJobID(long jobID){
		this.jobID = jobID;
	}

	public long getJobID() {
		return this.jobID;
	}

	public ControlMessage getMessage(){
		return message;
	}
	

}
