package distributed.systems.gridscheduler.messages;


/**
 * Message carrying a cluster load
 */
public abstract class LoadMessage extends ControlMessage {
    /**
    * Generated serial version UID
    */
    private static final long serialVersionUID = -1453428681740343634L;
    private float load;

    public LoadMessage(String senderName, float load){
        super(senderName);
        this.load = load;
    }

    /**
     * @return load of this RM
     */
    public float getLoad(){
        return load;
    }

    public static class ClusterLoadMessage extends LoadMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        public ClusterLoadMessage(String senderName, float load) {
            super(senderName, load);
        }
    }

}