package distributed.systems.gridscheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Random;

import javax.swing.JFrame;

import distributed.systems.core.RemoteSocket;
import distributed.systems.gridscheduler.gui.ClusterStatusPanel;
import distributed.systems.gridscheduler.gui.GridSchedulerPanel;
import distributed.systems.gridscheduler.gui.GridSchedulerStatusPanel;
import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.GridScheduler;
import distributed.systems.gridscheduler.model.Job;
import distributed.systems.gridscheduler.util.AddJobAction;
import distributed.systems.gridscheduler.util.CrashAction;
import distributed.systems.gridscheduler.util.GWFFileReader;
import distributed.systems.gridscheduler.util.GWFFileWriter;
import distributed.systems.gridscheduler.util.IWorkloadProvider;
import distributed.systems.gridscheduler.util.RandomWorkloadProvider;
import distributed.systems.gridscheduler.util.WorkloadAction;

/**
 *
 * The Simulation class is an example of a grid computation scenario. Every 100 milliseconds 
 * a new job is added to first cluster. As this cluster is swarmed with jobs, it offloads
 * some of them to the grid scheduler, wich in turn passes them to the other clusters.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 */
public class Simulation implements Runnable {
	// Number of Jobs to complete, Scalability requirement: 10.000
	private final static int nrJobsTotal = 1400;

	// Number of clusters in the simulation, Scalability requirement: 20
	private int nrClusters = 0;

	// Number of nodes per cluster in the simulation, Scalability requirement: 1000
	private final static int nrNodes = 100;

	private String workloadFileLocation;
	private IWorkloadProvider workloadProvider;
	private GWFFileWriter workloadWriter;

	// Simulation components
	ArrayList<Cluster> clusters;
	GridScheduler gridScheduler;
	GridSchedulerStatusPanel gridPanel;
	GridSchedulerPanel mainPanel;

	ArrayList<String> urlsRM;

	/**
	 * Constructs a new simulation object. Study this code to see how to set up your own
	 * simulation.
	 */
	public Simulation(int id, int entity, String gwfLocation, boolean restart) {
		mainPanel = new GridSchedulerPanel();
		mainPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Parse from file
		ArrayList<String> urlsGS = new ArrayList<String>();
		urlsRM = new ArrayList<String>();

		try {
			BufferedReader sc = new BufferedReader(new FileReader(
					new File("").getAbsolutePath() + File.separator + "settings" + File.separator + "gs.config"));
			String line;
			while ((line = sc.readLine()) != null) {
				urlsGS.add(line);
			}
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			BufferedReader sc = new BufferedReader(new FileReader(
					new File("").getAbsolutePath() + File.separator + "settings" + File.separator + "rm.config"));
			String line;
			while ((line = sc.readLine()) != null) {
				urlsRM.add(line);
			}
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		nrClusters = urlsRM.size();

		// JOB READER
		if (entity == 0) {
			System.out.println("Job adder wait for everyone to start (4s)");
			try {
				Thread.sleep(4000);
			} catch (InterruptedException e) {

			}
			workloadFileLocation = gwfLocation;
			try {
				workloadWriter = new GWFFileWriter();
				if (!workloadFileLocation.equals("")) {
					workloadProvider = new GWFFileReader(workloadFileLocation, 100);
				} else {
					workloadProvider = new RandomWorkloadProvider(nrClusters);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

			// Run the simulation
			Thread runThread = new Thread(this);
			runThread.run(); // This method only returns after the simulation has ended
			// Now perform the cleanup
			try {
				workloadWriter.close();
				System.out.println("writing file");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// I am a GS
		if (entity == 1) {
			gridScheduler = new GridScheduler(urlsGS.get(id), urlsGS, restart);
			gridPanel = new GridSchedulerStatusPanel(gridScheduler);
			mainPanel.addStatusPanel(gridPanel);
			System.out.println("Started a gridscheduler");
		}

		// I am a RM
		if (entity == 2) {
			Cluster c = new Cluster(urlsRM.get(id), urlsGS, nrNodes, restart);
			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(c);
			mainPanel.addStatusPanel(clusterReporter);
			System.out.println("cluseter added to mainpanel");
		}

		// // Open the gridscheduler panel and connect GridSchedulers to each other

		if (entity != 0) {
			System.out.println("starting entity: " + entity);
			mainPanel.start();
		}
		//TODO join threads before exit

		while (true) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {

			}
		}

	}

	/**
	 * The main run thread of the simulation. You can tweak or change this code to produce
	 * different simulation scenarios. 
	 */
	public void run() {
		WorkloadAction action;
		int counter = 0;
		// Do not stop the simulation as long as the gridscheduler panel remains open
		while (counter<1000) {
		// while(true){
			// Add a new job to the system that take up random time
			try {
				if ((action = workloadProvider.getNextWorkloadAction()) != null) {
					try {
						Thread.sleep(action.getSubmitTime());
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(2);
					}

					if (action instanceof AddJobAction) {
						AddJobAction workload = (AddJobAction) action;
						int clusterID = workload.getClusterID() % nrClusters;
						RemoteSocket sock = ((RemoteSocket) Naming.lookup(urlsRM.get(clusterID)));
					
						System.out.println(workload.getJob().getId() + " to " +  clusterID);
						addJob(sock, workload.getJob());
					
					} else if (action instanceof CrashAction) {
						CrashAction crash = (CrashAction) action;
						// gridSchedulers.get(crash.getNodeID()).kill();
					}
					workloadWriter.write(action);
				} else {
					try {
						Thread.sleep(100L);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
			counter++;

		}

		// System.out.println("main thread finished");

	}

	public void addJob(RemoteSocket sock, Job job){
		try{
			sock.addJob(job);
		}catch(RemoteException e){
			int clusterID = Math.abs(new Random().nextInt()) % nrClusters;
			try{
				sock =  ((RemoteSocket) Naming.lookup(urlsRM.get((clusterID))));
			}catch(Exception ex){
				ex.printStackTrace();
				return;
			}
			addJob(sock, job);
		}
	}

	/**
	 * Application entry point.
	 * @param args application parameters
	 */
	public static void main(String[] args) {

		// Create and run the simulation
		assert (args.length >= 3);
		boolean restart = Boolean.parseBoolean(args[0]);
		int id = Integer.parseInt(args[1]);
		int entity = Integer.parseInt(args[2]);
		String fileLocation = "";
		if (args.length > 3) {
			fileLocation = args[3];
		}

		// TODO: Maybe start every GS/RM on different port or start rmiregisty seperately
		// try {
		// 	LocateRegistry.createRegistry(5000);
		// } catch (RemoteException e) {
		// 	System.out.println("registry already running on this machine");
		// 	e.printStackTrace();
		// }

		new Simulation(id, entity, fileLocation, restart);
	}

}
