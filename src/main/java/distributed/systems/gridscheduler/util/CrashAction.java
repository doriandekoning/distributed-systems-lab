package distributed.systems.gridscheduler.util;

public class CrashAction extends WorkloadAction {
    int nodeID;
    
    //Can be used for identifiying a crash (in debugging for example)
    String name;
    Boolean gridSchedulerCrash;
    
    public CrashAction(long submitTime, boolean gridSchedulerCrash, int nodeID, String name){
        super(submitTime);
        this.name = name;
        this.gridSchedulerCrash = gridSchedulerCrash;
        this.nodeID = nodeID;
    }

    public int getNodeID(){
        return nodeID;
    }

    public String getCrashName(){
        return name;
    }

    public boolean isGridSchedulerCrash(){
        return gridSchedulerCrash;
    }

}