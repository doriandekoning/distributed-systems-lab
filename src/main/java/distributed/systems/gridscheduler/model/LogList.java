package distributed.systems.gridscheduler.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import distributed.systems.gridscheduler.messages.JobMessage.JobRunningMessage;


//TODO maybe it's an idea to keep track of the lowest jobid of the noncompleted jobs (all jobs with a lower jobid have been completed)
//this saves time, kindof like a snapshot
public class LogList {

    private ConcurrentHashMap<Long, Log> myLog;
    private long largestSequenceID = -1;

    public LogList() {
        this.myLog = new ConcurrentHashMap<Long, Log>();
    }

    public void addLogItem(Log l) {
        myLog.put(l.getJob().getId(), l);
        largestSequenceID = Math.max(largestSequenceID, l.getSequenceID());
    }

    public long getLargestSequenceID(){
        return largestSequenceID;
    }

    public ConcurrentHashMap<Long, Log> getLog() {
        return myLog;
    }

    public ArrayList<Log> getQueue(JobStatus status) {
        ArrayList<Log> result = new ArrayList<Log>();
        Iterator<Long> iter = myLog.keys().asIterator();
		while (iter.hasNext()) {
            Log l = myLog.get(iter.next());
            if (l.getJob().getStatus() == status) {
                result.add(l);
            }
        }
        Collections.sort(result);
        return result;
    }

    public ArrayList<Log> getDispatchedToRM() {
        ArrayList<Log> result = new ArrayList<Log>();
        Iterator<Long> iter = myLog.keys().asIterator();
		while (iter.hasNext()) {
            Log l = myLog.get(iter.next());
            if (l.getJob().getStatus() == JobStatus.DispatchedToRM) {
                result.add(l);
            }
        }
        Collections.sort(result);
        return result;
    }

    public int getSize() {
        return myLog.size();
    }

    public Job getUnfinishedJobForNode(int nodeid){
        Iterator<Long> iter = myLog.keys().asIterator();
        HashMap<Job, Long> jobsRunning = new HashMap<>();
		while (iter.hasNext()) {
            Log l = myLog.get(iter.next());
            if(l.getNodeID() == nodeid){
                jobsRunning.put(l.getJob(), l.getSequenceID());
            }
        }
        //After all jobs have been added if theyre running remove done jobs
        while (iter.hasNext()) {
            Log l = myLog.get(iter.next());
            if(l.getJob().getStatus() == JobStatus.Done){
                jobsRunning.remove(l.getJob());
            }
        }
        Job unfinishedJob = null;
        long unfinishedJobsequenceId = -1;
        //Find unfinished job with highest sequenceid
        for(Job job : jobsRunning.keySet()){
            if(jobsRunning.get(job) > unfinishedJobsequenceId){
                unfinishedJobsequenceId = jobsRunning.get(job);
                unfinishedJob = job;
            }
        }
        // System.out.println("Job"+ unfinishedJob.getId() + "Was not finished when node crashed");
        return unfinishedJob;
    }

}
