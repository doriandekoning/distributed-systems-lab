package distributed.systems.gridscheduler.model;

import java.io.Serializable;

/**
 * This class represents a job that can be executed on a grid. 
 * 
 * @author Niels Brouwers
 *
 */
public class Job implements Serializable {
	
	final static long serialVersionUID = 0;
	private long duration;
	private JobStatus status;
	private long id;
	private String gsURL;
	//Resourcemanager it orriginally arived in
	private String firstRMName;
	private Long requestToDispatchTimestamp;

	private long createTime;
	private String rmURL;

	/**
	 * Constructs a new Job object with a certain duration and id. The id has to be unique
	 * within the distributed system to avoid collisions.
	 * <P>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>duration</CODE> should be positive
	 * </DL> 
	 * @param duration job duration in milliseconds 
	 * @param id job ID
	 */
	public Job(long duration, long id) {
		// Preconditions
		assert(duration > 0) : "parameter 'duration' should be > 0";

		this.createTime = System.currentTimeMillis();
		this.duration = duration;
		this.status = JobStatus.Waiting;
		this.id = id; 
	}

	public void setRequestTimeStamp(Long ms) {
		this.requestToDispatchTimestamp = ms;
	} 

	public Long getRequestTimeStamp() {
		return this.requestToDispatchTimestamp;
	}

	public void setReceiverRM(String url) {
		this.rmURL = url;
	}

	public String getReceiverRM() {
		return this.rmURL;
	}

	/**
	 * Returns the duration of this job. 
	 * @return the total duration of this job
	 */
	public long getDuration() {
		return duration;
	}

	/**
	 * Returns the status of this job.
	 * @return the status of this job
	 */
	public JobStatus getStatus() {
		return status;
	}

	/**
	 * Sets the status of this job.
	 * @param status the new status of this job
	 */
	public void setStatus(JobStatus status) {
		if(status == JobStatus.Done){
			System.out.println("Time waited: " + (System.currentTimeMillis() - duration - createTime));
		}
		this.status = status;
	}

	/**
	 * The message ID is a unique identifier for a message. 
	 * @return the message ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return a string representation of this job object
	 */
	public String toString() {
		return "Job {ID :" + id + ", duration:"+ duration +"}";
	}

	public void setGS(String url) {
		this.gsURL = url;
	}

	public boolean fromGS() {
		return this.gsURL != null;
	}

	public String getGS() {
		return this.gsURL;
	}

	public void setFirstRMName(String name){
		//Can only be set once
		assert(this.firstRMName == null) : "First resource manager name of a job can only be set once";
		this.firstRMName = name;
	}

	public String getFirstRMName(){
		return this.firstRMName;
	}
	

}
