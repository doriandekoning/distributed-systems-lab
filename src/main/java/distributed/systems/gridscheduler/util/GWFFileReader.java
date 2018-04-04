package distributed.systems.gridscheduler.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import distributed.systems.gridscheduler.model.Job;

public class GWFFileReader implements IWorkloadProvider {

    private long previousSubmitTime = -1;

    private ArrayList<String> submitSiteNames;
    private int speedup = 1;

    BufferedReader bufferedReader;

    public GWFFileReader(String filename, int speedup) throws FileNotFoundException {
        assert(speedup > 0) : "Speedup must be bigger than 0";
        bufferedReader = new BufferedReader(new FileReader(filename));
        submitSiteNames = new ArrayList<>();
    }

    public WorkloadAction getNextWorkloadAction() {
        String line = null;
        try{
            line = readLine();
        }catch(IOException e){
            e.printStackTrace();
        }
        if(line == null || line == ""){
            return null;
        }
        String[] splitLine = line.split("[ \t]+");
        // Check if this line is a job or a crash
        if (splitLine[0].charAt(0) == '!') {
            //TODO refactor into parsecrash function
            //Example crash line format: "! [submitTime] [name] [gridscheduler(boolean)] [id]"
            //Line indicates a crash
            String crashName = splitLine[2];
            long submitTime = Long.parseLong(splitLine[1]) / speedup;
            boolean gridSchedulerCrash = Boolean.parseBoolean(splitLine[3]);
            int crashNodeId = Integer.parseInt(splitLine[4]);
            return new CrashAction(submitTime, gridSchedulerCrash, crashNodeId, crashName);
        } else {
            Job job = new Job( Math.max(Long.parseLong(splitLine[3])*1000 / speedup, 6000L), Integer.parseInt(splitLine[0]));
            long submitTime = Long.parseLong(splitLine[1]) / speedup;
            //Find out which cluster this job arrived
            String submitSite = splitLine[16];
            int clusterID = submitSiteNames.indexOf(submitSite);
            if (clusterID == -1) {
                clusterID = submitSiteNames.size();
                submitSiteNames.add(submitSite);
            }
            long waitTime = 0;
            if (previousSubmitTime != -1) {
                waitTime = submitTime - previousSubmitTime;
                previousSubmitTime = submitTime;
            } else {
                previousSubmitTime = submitTime;
            }
            return new AddJobAction(waitTime, job, clusterID);
        }
    }

    public String readLine() throws IOException {
        String readLine = bufferedReader.readLine();
        if(readLine != ""){
            //Skip commented lines
            while (readLine.charAt(0) == '#') {
                readLine = bufferedReader.readLine();
            }
        }
        return readLine;
    }
}