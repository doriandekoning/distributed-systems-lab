package distributed.systems.gridscheduler.model;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import distributed.systems.core.IAddJobHandler;
import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.RemoteSocket;
import distributed.systems.core.Socket;
import distributed.systems.gridscheduler.messages.AcknowledgementMessage;
import distributed.systems.gridscheduler.messages.ControlMessage;
import distributed.systems.gridscheduler.messages.ControlMessage.RequestLoadMessage;
import distributed.systems.gridscheduler.messages.JobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.AddJobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.RequestToAddJobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.RequestToDispatch;
import distributed.systems.gridscheduler.messages.LoadMessage;
import distributed.systems.gridscheduler.messages.LogMessage.LogContainerMessage;
import distributed.systems.gridscheduler.messages.LogMessage.RequestLogMessage;
import distributed.systems.gridscheduler.messages.ResourceManagerMessage;

/**
 * This class represents a resource manager in the VGS. It is a component of a cluster, 
 * and schedulers jobs to nodes on behalf of that cluster. It will offload jobs to the grid
 * scheduler if it has more jobs waiting in the queue than a certain amount.
 * 
 * The <i>jobQueueSize</i> is a variable that indicates the cutoff point. If there are more
 * jobs waiting for completion (including the ones that are running at one of the nodes)
 * than this variable, jobs are sent to the grid scheduler instead. This variable is currently
 * defaulted to [number of nodes] + MAX_QUEUE_SIZE. This means there can be at most MAX_QUEUE_SIZE jobs waiting 
 * locally for completion. 
 * 
 * Of course, this scheme is totally open to revision.
 * 
 * @author Niels Brouwers, Boaz Pat-El, Dorian de Koning
 *
 */

public class ResourceManager implements INodeEventHandler, IMessageReceivedHandler, IAddJobHandler, Runnable {
	private Cluster cluster;
	private Queue<Job> jobQueue;
	private String socketURL;
	private int jobQueueSize;
	private int messageSequenceId = 0;
	public static final int MAX_QUEUE_SIZE = 32;
	private Map<String, RemoteSocket> sockets;
	private LinkedBlockingQueue<Message> messageQueue;
	private HashSet<Long> jobIDsWaitingForAck;
	private String url;
	private ArrayList<String> gridSchedulerURLs;


	// polling thread
	private Thread pollingThread, messageThread;

	private long simStartTime;

	private boolean running;
	private boolean recovering;
	private int gsLogReceived;
	private long previousTimePrint = -1;

	private ConcurrentHashMap<String, LogList> log;
	private ConcurrentHashMap<Long, Job> recoveryLog;

	private Queue<Job> recoveryQueue;

	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 */
	public ResourceManager(Cluster cluster, String url, ArrayList<String> gsURLS, boolean restart) {
		// preconditions
		assert (cluster != null);
		assert (url != null);

		this.simStartTime = System.currentTimeMillis();
		this.gridSchedulerURLs = gsURLS;
		this.cluster = cluster;
		this.url = url;
		this.socketURL = cluster.getURL();
		this.log = new ConcurrentHashMap<String, LogList>();

		// Number of jobs in the queue must be larger than the number of nodes, because
		// jobs are kept in queue until finished. The queue is a bit larger than the 
		// number of nodes for efficiency reasons - when there are only a few more jobs than
		// nodes we can assume a node will become available soon to handle that job.
		jobQueueSize = cluster.getNodeCapacity() + MAX_QUEUE_SIZE;
		// Setup own socket
		Socket socket;
		try {
			socket = new Socket();
			socket.register(socketURL);
			socket.addMessageReceivedHandler(this);
			socket.addJobHandler(this);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// start the polling thread
		pollingThread = new Thread(this);
		pollingThread.start();

		Initialize(restart);
	}

	/**
	* Initialize initializes the GS
	*/
	private void Initialize(boolean restart) {
		boolean recovered = false;

		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.messageQueue = new LinkedBlockingQueue<>();
		this.jobIDsWaitingForAck = new HashSet<>();
		this.recoveryQueue = new ConcurrentLinkedQueue<Job>();
		this.recoveryLog = new ConcurrentHashMap<Long, Job>();

		connectToGridSchedulers();
		if (restart) {
			recoverOldLogRequest();
			this.recovering = true;
		}

		messageThread = new Thread() {
			public void run() {
				while (running || recovering) {

					if(previousTimePrint == -1 || previousTimePrint + 1000 < System.currentTimeMillis()){
						System.out.println("Current load:"+ getLoad());
						previousTimePrint = System.currentTimeMillis();
					}
					System.out.println("waitinforack" +  jobIDsWaitingForAck.size());
					try {
						handleMessage(messageQueue.poll(1, TimeUnit.SECONDS));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		messageThread.start();

		while (restart && !recovered) {
			if (this.gsLogReceived >= (gridSchedulerURLs.size())) {
				for (Job j : recoveryLog.values()) {
					this.jobQueue.add(j);
				}
				recovered = true;
				System.out.println(recoveryLog.size() + " size of recovered queue");
				System.out.println("Restarted: " + cluster.getURL());
			}
			// recover log
			try {
				System.out.println("waiting for response");
				Thread.sleep(Math.abs(4000));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (restart) {
			ControlMessage message = new ControlMessage.ResourceManagerJoinMessage(this.url, cluster.getNodeCapacity());
			ResourceManagerMessage rmMessage = new ResourceManagerMessage(message, getLoad());
			for (String socket : sockets.keySet()) {
				sendMessage(socket, rmMessage);
			}
		}

		// start the polling thread
		while (recoveryQueue.peek() != null) {
			this.jobQueue.add(recoveryQueue.remove());
		}
		this.recovering = false;
		this.running = true;
	}

	private void recoverOldLogRequest() {
		RequestLogMessage request = new RequestLogMessage(url, 2);
		for (String socket : sockets.keySet()) {
			System.out.println("logs received:"+ this.gsLogReceived);
			sendMessage(socket, request);
		}
	}

	public void run() {
		while (true) {
			scheduleJobs();
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {

			}
		}
	}

	/**
	 * Add a job to the resource manager. If there is a free node in the cluster the job will be
	 * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
	 * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>job</CODE> cannot be null
	 * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
	 * connected to a grid scheduler)
	 * </DL>
	 * @param job the Job to run
	 */
	public void addJob(Job job) {
		// check preconditions
		assert (job != null) : "the parameter 'job' cannot be null";

		job.setFirstRMName(this.url);

		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= cluster.getNodeCapacity() || recovering) {
			if (recovering) {
				System.out.println("offload job while recovering");
			}
			job.setStatus(JobStatus.Waiting);
			JobMessage mesg = new AddJobMessage(url, job);
			ResourceManagerMessage controlMessage = new ResourceManagerMessage(mesg, getLoad());
			saveToLog(mesg);
			jobIDsWaitingForAck.add(job.getId());
			int randomNum = ThreadLocalRandom.current().nextInt(0, gridSchedulerURLs.size());
			if (gridSchedulerURLs.get(randomNum) != null) {
				RequestToAddJobMessage request = new RequestToAddJobMessage(gridSchedulerURLs.get(randomNum), job);
				saveToLog(request);
				for (String gs : sockets.keySet()) {
					sendMessage(gs, request);
				}
				while (!sendMessage(gridSchedulerURLs.get(randomNum), controlMessage)) {
					System.out.println("failed to send to: " + gridSchedulerURLs.get(randomNum));
					randomNum = ThreadLocalRandom.current().nextInt(0, gridSchedulerURLs.size());
				}
			} else {
				addJob(job);
			}
			// otherwise store it in the local queue
		} else {
			//TODO log this
			job.setStatus(JobStatus.DispatchedToRM);
			RequestToDispatch jm = new RequestToDispatch(url, job, url);
			for (String rm : gridSchedulerURLs) {
				sendMessage(rm, jm);
			}
			jobQueue.add(job);
			scheduleJobs();
		}

	}

	/**
	 * Tries to find a waiting job in the jobqueue.
	 * @return
	 */
	public Job getWaitingJob() {
		// find a waiting job
		for (Job job : jobQueue)
			if (job.getStatus() == JobStatus.Waiting || job.getStatus() == JobStatus.DispatchedToRM)
				return job;

		// no waiting jobs found, return null
		return null;
	}

	/**
	 * Tries to schedule jobs in the jobqueue to free nodes. 
	 */
	public void scheduleJobs() {
		// while there are jobs to do and we have nodes available, assign the jobs to the 
		// free nodes
		Node freeNode;
		Job waitingJob;

		while (((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null)) {
			if (freeNode.getStatus() != NodeStatus.Down) {
				freeNode.startJob(waitingJob);
				JobMessage message = new JobMessage.JobRunningMessage(url, messageSequenceId++, waitingJob,
						freeNode.getId());
				saveToLog(message);
				//TODO make single 
				broadCastMessage(message);
			}
		}
	}

	/**
	 * Called when a job is finished
	 * <p>
	 * pre: parameter 'job' cannot be null
	 */
	public void jobDone(Job job) {
		// preconditions
		assert (job != null) : "parameter 'job' cannot be null";

		// if (job.fromGS()) {
		//TODO is this broadcast really needed? or can it be a multicastb
		System.out.println("Job ID: " + job.getId() + ", stopped after:" + (System.currentTimeMillis() - simStartTime));

		JobMessage message = new JobMessage.JobDoneMessage(url, job);
		saveToLog(message);
		broadCastMessage(message);
		// }
		// job finished, remove it from our pool
		jobQueue.remove(job);
	}

	/**
	 * @return the url of the grid scheduler this RM is connected to 
	 */
	public ArrayList<String> getGridSchedulerURLs() {
		return gridSchedulerURLs;
	}

	private void broadCastMessage(ControlMessage status) {
		for (String gsName : sockets.keySet()) {
			sendMessage(gsName, new ResourceManagerMessage(status, getLoad()));
		}
	}

	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'gridSchedulerURL' must not be null
	 * @param gridSchedulerURL
	 */
	public void connectToGridSchedulers() {
		sockets = new HashMap<>();
		for (String url : gridSchedulerURLs) {
			boolean success = false;
			while (!success) {
				try {
					// sockets.put(url, (RemoteSocket) LocateRegistry.getRegistry(url.split("/")[2].split(":")[0], 5000)
					// 		.lookup(url));
					sockets.put(url, (RemoteSocket) Naming.lookup(url));
					success = true;
				} catch (Exception e) {

					//Sleep and try again if it comes online
					try {
						Thread.sleep(1000);
					} catch (InterruptedException iException) {
						iException.printStackTrace();
					}
				}
			}

			assert (sockets.get(url) != null) : "gone wrong";
			ControlMessage message = new ControlMessage.ResourceManagerJoinMessage(this.url, cluster.getNodeCapacity());
			ResourceManagerMessage rmMessage = new ResourceManagerMessage(message, getLoad());
			sendMessage(url, rmMessage);
		}
	}

	/**
	 * Message received handler
	 */
	public void onMessageReceived(Message message) {

		try {
			this.messageQueue.put(message);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*
	* <p>
	* pre: parameter 'message' should be of type ControlMessage 
	* pre: parameter 'message' should not be null 
	* @param message a message
	*/
	public void handleMessage(Message message) {
		// preconditions
		assert (message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert (message != null) : "parameter 'message' cannot be null";

		if (message instanceof AcknowledgementMessage) {
			handleAcknowledgement((AcknowledgementMessage) message);
		}
		if (message instanceof LogContainerMessage) {
			// TODO: log from other GS may contain somse jobs that were already scheduled, find out why
			// Probably to do with RM not broadcasting running status anymore or something?
			LogContainerMessage m = (LogContainerMessage) message;
			int counterRemoved = 0;
			int counterDispatched = 0;
			for (Log l : m.getLog()) {
				if (recoveryLog.get(l.getJob().getId()) == null && l.getJob().getStatus() != JobStatus.Done) {
					counterDispatched++;
					recoveryLog.put(l.getJob().getId(), l.getJob());
				} else if (recoveryLog.get(l.getJob().getId()) != null && l.getJob().getStatus() == JobStatus.Done) {
					counterRemoved++;
					recoveryLog.remove(l.getJob().getId());
				}
			}
			System.out.println("Dispatched added were: " + counterDispatched + " and removed from the list were: " + counterRemoved);
			gsLogReceived += 1;
		} else if (message instanceof ControlMessage) {
			handleControlMessage((ControlMessage) message);
		}
	}

	public void handleControlMessage(ControlMessage controlMessage) {
		// resource manager wants to offload a job to us 
		if (controlMessage instanceof JobMessage.AddJobMessage) {
			jobQueue.add(((AddJobMessage) controlMessage).getJob());
			scheduleJobs();
		}

		// grid scheduler wants to know our load
		if (controlMessage instanceof RequestLoadMessage) {
			ResourceManagerMessage replyMessage = new ResourceManagerMessage(
					new LoadMessage.ClusterLoadMessage(url, getLoad()), getLoad());
			// int randomNum = ThreadLocalRandom.current().nextInt(0, gridSchedulerSocket.length);
			// RemoteSocket s = gridSchedulerSocket[randomNum];
			sendMessage(controlMessage.getSenderName(), replyMessage);
		}

	}

	public void handleAcknowledgement(AcknowledgementMessage ack) {
		//Currently only one type of acks get send to rm
		//TODO timeouts for acks
		jobIDsWaitingForAck.remove(ack.getJobID());
	}

	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert (false) : "RM stopPollThread was interrupted";
		}
		try {
			messageThread.join();
		} catch (InterruptedException ex) {
			assert (false) : "Rm stopPollThread was interrupted while stopping";
		}

	}

	private boolean sendMessage(String receiver, Message message) {
		message.setSequenceID(messageSequenceId++);
		RemoteSocket receiverSocket = sockets.get(receiver);
		if (receiverSocket == null) {
			System.out.println("Trying to send message to unknown receiver: " + receiver);
			return true;
		}
		try {
			receiverSocket.sendMessage(message);
		} catch (RemoteException e) {
			//If sending message fails try to reinstantiate the socket
			System.out.println("failed to send message");
			//Find socket
			try {
				// sockets.put(receiver, (RemoteSocket) LocateRegistry.getRegistry(url.split("/")[2].split(":")[0], 5000)
				// 		.lookup(receiver));
				sockets.put(receiver, (RemoteSocket) Naming.lookup(receiver));
				receiverSocket.sendMessage(message);
			} catch (Exception exception) {
				return false;
			}

		}
		return true;

	}

	public float getLoad() {
		if (recovering) {
			return Float.MAX_VALUE;
		}
		return (float) jobQueue.size() / (cluster.getNodeCapacity() - cluster.getCrashedNodes());
	}

	private void saveToLog(JobMessage message) {
		Log logItem = new Log(message);
		LogList logItems = log.get(message.getSenderName());
		if (logItems == null) {
			logItems = new LogList();
		}
		logItems.addLogItem(logItem);
		log.put(message.getSenderName(), logItems);
	}

	public void nodeCrashed(int nodeID) {
		// Find out which job was running on the node
		// if (log.get(url) != null) {
		// 	Job unfinishedJob = log.get(url).getUnfinishedJobForNode(nodeID);
		// 	if (unfinishedJob != null) {
		// 		jobQueue.add(unfinishedJob);
		// 	}
		// }
	}
}
