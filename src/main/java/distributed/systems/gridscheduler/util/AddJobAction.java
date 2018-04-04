package distributed.systems.gridscheduler.util;

import distributed.systems.gridscheduler.model.Job;

public class AddJobAction extends WorkloadAction {
    Job job;
    int clusterID;
    
    public AddJobAction(long submitTime, Job job, int clusterID){
        super(submitTime);
        this.job = job;
        this.clusterID = clusterID;
    }

    public long getSubmitTime(){
        return submitTime;
    }

    public Job getJob(){
        return job;
    }

    public int getClusterID(){
        return clusterID;
    }

}