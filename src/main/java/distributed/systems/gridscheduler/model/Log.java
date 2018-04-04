package distributed.systems.gridscheduler.model;

import distributed.systems.gridscheduler.messages.JobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.JobRunningMessage;

import java.io.Serializable;

/**
 * Class represents a log item
 * @author Rick Proost, Dorian de Koning
 */
public class Log implements Comparable<Log>, Serializable {

    private static final long serialVersionUID = -7100263864222434885L;
	private String url;
    private Job job;
    private long sequenceID;
    private int nodeID;

    public Log(JobMessage message) {
        this.url = message.getSenderName();
        this.job = message.getJob();
        this.sequenceID = message.getSequenceID();
        if(message instanceof JobRunningMessage){
            this.nodeID = ((JobRunningMessage)message).getNodeID();
        }else{
            this.nodeID = -1;
        }
    }

    public String getUrl() {
        return this.url;
    }

    public Job getJob() {
        return this.job;
    }

    public int getNodeID(){
        return nodeID;
    }
    public long getSequenceID() {
        return this.sequenceID;
    }

    @Override
    public int compareTo(Log other) {
        return Long.valueOf(sequenceID).compareTo(other.getSequenceID());
    }

    @Override
    public String toString() {
        return "Log from GS: " +getUrl() + " with job ID: " + getJob().getId() + " with sequence ID: " + getSequenceID() + " job status: " + job.getStatus();
    }

}
