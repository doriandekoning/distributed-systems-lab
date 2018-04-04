package distributed.systems.core;

import distributed.systems.gridscheduler.model.Job;

public interface IAddJobHandler{
    public void addJob(Job job);
}