package distributed.systems.gridscheduler.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import distributed.systems.gridscheduler.model.Job;

public class RandomWorkloadProvider implements IWorkloadProvider {

	private final static boolean RANDOM_CRASHES_ENABLED = true;
	private int nrClusters;
	private long jobId = 0;

	// Counters for ratio 5:1
	private int counterMost = 0;
	private int counterLeast = 0;

	public RandomWorkloadProvider(int nrClusters) {
		this.nrClusters = nrClusters;
	}

	public WorkloadAction getNextWorkloadAction() {
		//Randomly crash a GS node
		if (RANDOM_CRASHES_ENABLED) {
			//TODO Crashing
		}

		Job job = new Job(5000 + (int) (ThreadLocalRandom.current().nextLong() % 5000), jobId++);
		//TODO burstiness maybe
		long submitTime = Math.abs(22L);
		return new AddJobAction(submitTime, job, getClusterToOffLoad());

	}

	private int getClusterToOffLoad() {
		int clusterLast = nrClusters - 1;
		int clustNotLast = nrClusters - 2;
		int semiRandomNum = 0;
		// int coinFlip = ThreadLocalRandom.current().nextInt(0, 10);
		// if (coinFlip < 8) {
		// 	// Make sure realistic imbalance with ratio most/least 5:1
		// 	if (counterMost < (7 * counterLeast) && counterLeast != 0) {
		// 	semiRandomNum = clusterLast;
		// 	counterMost++;
		// 	} else {
		// 		semiRandomNum = clustNotLast;
		// 		counterLeast++;
		// 	}
		// } else {
			// Now jobs get evenly added to clusters only not the last two one
			semiRandomNum = ThreadLocalRandom.current().nextInt(0, nrClusters);
		// }
		return semiRandomNum;
	}
}