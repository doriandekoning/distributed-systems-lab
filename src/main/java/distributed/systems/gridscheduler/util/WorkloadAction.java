package distributed.systems.gridscheduler.util;

public class WorkloadAction {
    long submitTime;

    public WorkloadAction(long submitTime) {
        this.submitTime = submitTime;
    }

    public long getSubmitTime() {
        return submitTime;
    }

}