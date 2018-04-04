package distributed.systems.gridscheduler.messages;

import distributed.systems.gridscheduler.model.Job;

/**
 * Message carrying a job.
 */
public abstract class JobMessage extends ControlMessage {
    public static enum MessageType {
        AddJob, RequestToAddJob, JobDone, JobRunning
    }

    /**
    * Generated serial version UID
    */
    private static final long serialVersionUID = -1453428681740343634L;
    protected Job job;
    private MessageType messageType;

    public JobMessage(String senderName, Job job) {
        super(senderName);
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job j) {
        this.job = j;
    }

    public void setSender(String url) {
        this.senderName = url;
    }

    public MessageType getMessageType() {
        return this.messageType;
    }

    public static class AddJobMessage extends JobMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        public AddJobMessage(String senderName, Job job) {
            super(senderName, job);
        }
    }

    public static class JobDoneMessage extends JobMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        public JobDoneMessage(String senderName,Job job) {
            super(senderName, job);
        }
    }

    public static class JobRunningMessage extends JobMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        private int nodeID;
        public JobRunningMessage(String senderName, long sequenceID, Job job, int nodeID) {
            super(senderName, job);
            this.nodeID = nodeID;
        }

        public int getNodeID(){
            return this.nodeID;
        }
    }
    
    public static class RequestToAddJobMessage extends JobMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        public RequestToAddJobMessage(String senderName, Job job) {
            super(senderName, job);
        }
    }

    public static class RequestToDispatch extends JobMessage {
        private static final long serialVersionUID = -1453428681740343634L;
        private String receiver;
        public RequestToDispatch(String senderName, Job job, String receiver) {
            super(senderName, job);
            this.receiver = receiver;
        }

        public String getReceiver(){
            return receiver;
        }
    }


}