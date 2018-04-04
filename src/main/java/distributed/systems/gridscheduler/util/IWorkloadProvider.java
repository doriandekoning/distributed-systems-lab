package distributed.systems.gridscheduler.util;


public interface IWorkloadProvider{
   public WorkloadAction getNextWorkloadAction();
}