package distributed.systems.gridscheduler.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;

import distributed.systems.gridscheduler.model.Job;

public class GWFFileWriter {

    private long previousSubmitTime = -1;
    private static final String JOB_OUTPUT_FORMAT = "%d\t%d\t-1.0\t%d\t1\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t-1.0\t%d\t";
    private static final String CRASH_OUTPUT_FORMAT = "!\t%d\t%d\t%d\t%s\t";

    BufferedWriter bufferedWriter;

    public GWFFileWriter() throws IOException {
        DateFormat format = new SimpleDateFormat("dd-MM-yyyy-kk-mm-ss");
        String filename = "output-" + format.format(new Date())  + ".out";
        File dir = new File("out");
        if(!dir.exists()){
            dir.mkdir();
        }
        File file = new File("out" + File.separator + filename);
        if(!file.exists() && !file.isDirectory()){
            bufferedWriter = new BufferedWriter(new FileWriter(file));
        }else{
            throw new IOException("File exists");
        }
    }

    public void write(WorkloadAction action) throws IOException {
        if (previousSubmitTime == -1) {
            previousSubmitTime = 0;
        } else {
            previousSubmitTime += action.getSubmitTime();
        }
        if (action instanceof AddJobAction) {
            AddJobAction addJobAction = (AddJobAction) action;
            Job job = addJobAction.getJob();
            bufferedWriter.write(String.format(JOB_OUTPUT_FORMAT, job.getId(), previousSubmitTime*10, (int) job.getDuration()*10,
                    addJobAction.getClusterID()));
        } else if (action instanceof CrashAction) {
            CrashAction crashAction = (CrashAction) action;
            bufferedWriter.write("!" + String.format(CRASH_OUTPUT_FORMAT, previousSubmitTime*10,
                    previousSubmitTime, crashAction.getCrashName(), crashAction.getNodeID()));
        }
        bufferedWriter.newLine();

    }

    public void close() throws IOException {
        bufferedWriter.close();
    }
}