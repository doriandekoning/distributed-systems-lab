package distributed.systems.gridscheduler.messages;

import distributed.systems.core.Message;

/**
 * 
 * Class that represents the messages being sent from RM to GS nodes.
 * 
 * @author Dorian de Koning
 *
 */
public class ResourceManagerMessage implements Message {
    /**
    * Generated serial version UID
    */
    private static final long serialVersionUID = -1453428681740343634L;

    private float load;
    private ControlMessage message;

    public ResourceManagerMessage(ControlMessage message, float load) {
        this.message = message;
        this.load = load;
    }

    public ControlMessage getMessage() {
        return message;
    }

    public float getLoad() {
        return load;
    }


    public void setSequenceID(long sequenceID){
        message.setSequenceID(sequenceID);
    }

    public long getSequenceID(){
        return message.getSequenceID();
    }

}