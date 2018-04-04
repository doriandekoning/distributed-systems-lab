package distributed.systems.gridscheduler.messages;

import java.util.ArrayList;
import distributed.systems.gridscheduler.model.Log;

public abstract class LogMessage extends ControlMessage {
    /**
    * Generated serial version UID
    */
    private static final long serialVersionUID = -1453428681740343634L;

    public LogMessage(String senderName){
        super(senderName);
    }

    public static class RequestLogMessage extends LogMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        private int entity; // 1==grid, 2==rm
        public RequestLogMessage(String senderName, int entity) {
            super(senderName);
            this.entity = entity;
        }

        public int getEntity() {
            return entity;
        }
    }

    public static class LogContainerMessage extends LogMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        private ArrayList<Log> log;
        private long largestSequenceID;
        public LogContainerMessage(String senderName, ArrayList<Log> l, long largestSequenceID) {
            super(senderName);
            this.log = l;
            this.largestSequenceID = largestSequenceID;
        }

        public ArrayList<Log> getLog() {
            return log;
        }

        public long getLargestSequenceID(){
            return largestSequenceID;
        }

    }
}