package distributed.systems.gridscheduler.model;

import java.lang.reflect.Array;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.RemoteSocket;
import distributed.systems.core.Socket;
import distributed.systems.gridscheduler.messages.AcknowledgementMessage;
import distributed.systems.gridscheduler.messages.ControlMessage;
import distributed.systems.gridscheduler.messages.JobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.RequestToAddJobMessage;
import distributed.systems.gridscheduler.messages.JobMessage.RequestToDispatch;
import distributed.systems.gridscheduler.messages.LoadMessage;
import distributed.systems.gridscheduler.messages.LogMessage;
import distributed.systems.gridscheduler.messages.LogMessage.LogContainerMessage;
import distributed.systems.gridscheduler.messages.LogMessage.RequestLogMessage;
import distributed.systems.gridscheduler.messages.ResourceManagerMessage;
import distributed.systems.gridscheduler.messages.ControlMessage.ResourceManagerJoinMessage;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers, Dorian de Koning
 *
 */
public class GridScheduler implements IMessageReceivedHandler, Runnable {

	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	private ConcurrentLinkedQueue<Job> recoveryQueue;

	// Name
	private final String url;

	// communications socket
	private ConcurrentHashMap<String, RemoteSocket> resourceManagerSockets;
	private ConcurrentHashMap<String, RemoteSocket> gridSchedulerSockets;
	private ConcurrentHashMap<String, String> rmStatus;
	private ConcurrentHashMap<String, Integer> resourceManagerNodeCounts;

	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Float> resourceManagerLoad;

	private HashSet<Long> jobIDsWaitingForAck;
	private ConcurrentHashMap<Long, Job> jobsWaitingForDispatch;

	private LinkedBlockingQueue<Message> messageQueue;

	// polling frequency, 1hz
	private long pollSleep = 1000;

	// polling thread
	private Thread pollingThread, messageThread;
	private boolean running;
	private boolean recovering;
	private ArrayList<String> gsURLS;

	// Log
	private ConcurrentHashMap<String, LogList> distributedLog;
	private ArrayList<Log> recoveryLog;
	private int gsLogReceived;
	private long messageSequenceId;

	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>name</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 */
	public GridScheduler(String url, ArrayList<String> gsURLS, boolean restart) {
		// preconditions
		assert (url != null) : "parameter 'url' cannot be null";

		this.url = url;
		this.gsURLS = gsURLS;
		pollingThread = new Thread(this);
		this.running = true;
		// create a messaging socket
		try {
			Socket socket = new Socket();
			socket.register(url);
			socket.addMessageReceivedHandler(this);
			System.out.println("Registerd for messages: " + url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		pollingThread.start();

		Initialize(restart);
	}

	/**
	* Initialize initializes the GS
	*/
	private void Initialize(boolean restart) {
		boolean recovered = false;

		this.resourceManagerSockets = new ConcurrentHashMap<String, RemoteSocket>();
		this.resourceManagerNodeCounts = new  ConcurrentHashMap<>(); 
		this.gsLogReceived = 0;
		this.recoveryLog = new ArrayList<Log>();
		this.messageSequenceId = 0;
		this.resourceManagerLoad = new ConcurrentHashMap<String, Float>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.recoveryQueue = new ConcurrentLinkedQueue<Job>();
		this.distributedLog = new ConcurrentHashMap<String, LogList>();
		this.messageQueue = new LinkedBlockingQueue<>();
		this.jobIDsWaitingForAck = new HashSet<>();
		this.jobsWaitingForDispatch = new ConcurrentHashMap<>();
		this.rmStatus = new ConcurrentHashMap<String, String>();
		connectToGridSchedulers(gsURLS);
		if (restart) {
			recoverOldLogRequest();
			this.recovering = true;
		}

		messageThread = new Thread() {
			public void run() {
				while (running || recovering) {
					try {
						handleMessage(messageQueue.take());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("out");
			}
		};
		messageThread.start();

		while (restart && !recovered) {
			System.out.println("restarting...");
			if (this.gsLogReceived >= (Math.round(gsURLS.size() / 2))) {
				for (Log l : recoveryLog) {
					this.jobQueue.add(l.getJob());
					// System.out.println("adding job" + l.getJob().getId());
				}
				recovered = true;
				System.out.println("Restarted: " + getURL());
			}
			// recover log
			try {
				Thread.sleep(Math.abs(4000));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		this.recovering = false;
		// start the polling thread
		while (recoveryQueue.peek() != null) {
			this.jobQueue.add(recoveryQueue.remove());
		}
		System.out.println("Recovered");
		this.running = true;
	}

	/**
	* Connect to a grid scheduler
	* <p>
	* pre: the parameter 'gridSchedulerURL' must not be null
	* @param gridSchedulerURL
	*/
	public void connectToGridSchedulers(ArrayList<String> urls) {
		gridSchedulerSockets = new ConcurrentHashMap<>(urls.size());
		for (String url : urls) {
			System.out.println(url + " -- " + getURL());
			if (url.equals(getURL())) {
				continue;
			}
			boolean success = false;
			while (!success) {
				try {
					// gridSchedulerSockets.put(url, (RemoteSocket) LocateRegistry.getRegistry(url.split("/")[2].split(":")[0], 5000).lookup(url));
					gridSchedulerSockets.put(url, ((RemoteSocket) Naming.lookup(url)));
					success = true;
				} catch (Exception e) {
					System.out.println("not yet connected" + url);
					//Sleep and try again if it comes online
					try {
						Thread.sleep(1000);
					} catch (InterruptedException iException) {
						iException.printStackTrace();
					}
				}
			}
			assert (gridSchedulerSockets.get(url) != null) : "gone wrong";
		}
	}

	/**
	 * The gridscheduler's name also doubles as its name in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getURL() {
		return url;
	}

	/**
	 * Gets the number of jobs that are waiting for completion.
	 * @return
	 */
	public int getWaitingJobs() {
		int ret = 0;
		ret = jobQueue.size() + jobsWaitingForDispatch.size();
		return ret;
	}

	/**
	 * Gets the number of log items received by other GSs
	 */
	public int getLogItems() {
		int amountOfLogItems = 0;
		Iterator<String> iter = distributedLog.keys().asIterator();
		while (iter.hasNext()) {
			LogList log = distributedLog.get(iter.next());
			amountOfLogItems += log.getSize();
		}
		return amountOfLogItems;
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
		assert (message instanceof ControlMessage
				|| message instanceof ResourceManagerMessage) : "parameter 'message' should be of type ControlMessage or ResourceManagerMessage";
		assert (message != null) : "parameter 'message' cannot be null";
		if (message instanceof AcknowledgementMessage) {
			handleAcknowledgement((AcknowledgementMessage) message);
		} else if (message instanceof ResourceManagerMessage) {
			ResourceManagerMessage rmMessage = (ResourceManagerMessage) message;
			//Update load
			resourceManagerLoad.put(rmMessage.getMessage().getSenderName(), rmMessage.getLoad());
			handleMessage(((ResourceManagerMessage) message).getMessage());
		} else if (message instanceof LogMessage) {
			LogMessage m = (LogMessage) message;
			handleLogMessage(m);
		} else if (message instanceof ControlMessage) {
			handleControlMessage((ControlMessage) message);
		}

	}

	private void sendLogContainerToGS(LogMessage message) {
		System.out.println("Sending to GS");
		LogList schedulerLog = distributedLog.get(message.getSenderName());
		try {
			System.out.println(message.getSenderName());
			// gridSchedulerSockets.put(message.getSenderName(), (RemoteSocket) LocateRegistry.getRegistry(message.getSenderName().split("/")[2].split(":")[0], 5000).lookup(message.getSenderName()));
			gridSchedulerSockets.put(message.getSenderName(), (RemoteSocket) Naming.lookup(message.getSenderName()));
		} catch (Exception e) {
			// System.out.println("Sender of message not found");
		}

		ArrayList<Log> waitingJobs = new ArrayList<Log>();
		if (schedulerLog != null) {
			waitingJobs = schedulerLog.getQueue(JobStatus.Waiting);
		}
		RemoteSocket gs = gridSchedulerSockets.get(message.getSenderName());
		// System.out.println("Gettng LOG for " + message.getSenderName() + " log is size: " + waitingJobs.size());
		LogContainerMessage m = new LogContainerMessage(url, waitingJobs, schedulerLog.getLargestSequenceID());
		try {
			gs.sendMessage(m);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private void sendLogContainerToRM(LogMessage message) {
		LogList schedulerLog = distributedLog.get(message.getSenderName());
		try {
			rmStatus.put(message.getSenderName(), "Recovering");
			resourceManagerSockets.put(message.getSenderName(), (RemoteSocket) Naming.lookup(message.getSenderName()));
		} catch (Exception e) {
			System.out.println("Sender of message not found");
		}
		ArrayList<Log> doneAndDispatched = new ArrayList<Log>();
		doneAndDispatched.addAll(schedulerLog.getDispatchedToRM());
		doneAndDispatched.addAll(schedulerLog.getQueue(JobStatus.Running));
		doneAndDispatched.addAll(schedulerLog.getQueue(JobStatus.Waiting));
		doneAndDispatched.addAll(schedulerLog.getQueue(JobStatus.Done));
		RemoteSocket gs = resourceManagerSockets.get(message.getSenderName());
		LogContainerMessage m = new LogContainerMessage(url, doneAndDispatched, schedulerLog.getLargestSequenceID());

		try {
			gs.sendMessage(m);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void handleLogMessage(LogMessage message) {
		if (!message.getSenderName().equals(url)) {
			if (message instanceof RequestLogMessage) {
				RequestLogMessage rMessage = (RequestLogMessage) message;
				if (rMessage.getEntity() == 1) {
					sendLogContainerToGS(message);
				} else {
					sendLogContainerToRM(message);
				}
			} else if (message instanceof LogContainerMessage) {
				// TODO: log from other GS may contain somse jobs that were already scheduled, find out why
				// Probably to do with RM not broadcasting running status anymore or something?
				LogContainerMessage m = (LogContainerMessage) message;
				System.out.println("Ols seqID:"+ messageSequenceId + " new: "+ m.getLargestSequenceID());
				this.messageSequenceId = Math.max(m.getLargestSequenceID(), this.messageSequenceId);
				if (recoveryLog.size() > 0) {
					int counter = 0;
					for (Log lOld : recoveryLog) {
						if (lOld.getSequenceID() >= m.getLog().get(counter).getSequenceID()) {
							break;
						} else {
							// System.out.println(lOld.getSequenceID() + " en " + m.getLog().get(counter).getSequenceID());
						}
					}
				} else {
					recoveryLog = m.getLog();
				}
				System.out.println("log received");
				gsLogReceived += 1;
			}

		}
	}

	public void handleControlMessage(ControlMessage controlMessage) {
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage instanceof ControlMessage.ResourceManagerJoinMessage) {
			resourceManagerNodeCounts.put(controlMessage.getSenderName(), ((ControlMessage.ResourceManagerJoinMessage)controlMessage).getNodeCount());
			addResourceManager(controlMessage.getSenderName());
		} else if (controlMessage instanceof JobMessage.AddJobMessage) {
			Job job = ((JobMessage.AddJobMessage) controlMessage).getJob();
			sendRequestToAddJob(job);
			jobIDsWaitingForAck.add(job.getId());
			saveToLog((JobMessage) controlMessage);
		} else if (controlMessage instanceof JobMessage.RequestToAddJobMessage) {
			if (!controlMessage.getSenderName().equals(url)) {
				AcknowledgementMessage ack = new AcknowledgementMessage(url, controlMessage);
				Collection<RemoteSocket> receivers = new ArrayList<RemoteSocket>();
				receivers.add(gridSchedulerSockets.get(controlMessage.getSenderName()));
				sendMessage(receivers, ack);
				saveToLog((JobMessage) controlMessage);
			}
		} else if (controlMessage instanceof LoadMessage.ClusterLoadMessage) {
			resourceManagerLoad.put(controlMessage.getSenderName(),
					((LoadMessage.ClusterLoadMessage) controlMessage).getLoad());
		} else if (controlMessage instanceof JobMessage.RequestToDispatch) {
			if (!controlMessage.getSenderName().equals(url)) {
				RequestToDispatch rtdm = (RequestToDispatch) controlMessage;
				AcknowledgementMessage ack = new AcknowledgementMessage(url, controlMessage);
				Collection<RemoteSocket> receivers = new ArrayList<RemoteSocket>();
				receivers.add(gridSchedulerSockets.get(controlMessage.getSenderName()));
				sendMessage(receivers, ack);
				JobMessage jm = (JobMessage) controlMessage;
				Job j = jm.getJob();
				j.setStatus(JobStatus.DispatchedToRM);
				jm.setJob(j);
				jm.setSender(rtdm.getReceiver());
				saveToLog(jm);
			}
		}

		// If GS wants us to log his activity
		if (controlMessage instanceof JobMessage.JobDoneMessage
				|| controlMessage instanceof JobMessage.JobRunningMessage) {
			saveToLog((JobMessage) controlMessage);
		}
	}

	private void handleAcknowledgement(AcknowledgementMessage ack) {
		if (ack.getMessage() instanceof JobMessage.RequestToAddJobMessage) {
			//If we're still waiting for this job to be added
			Job job = ((RequestToAddJobMessage) ack.getMessage()).getJob();
			if (this.jobIDsWaitingForAck.contains(job.getId())) {
				this.jobIDsWaitingForAck.remove(job.getId());
				if (recovering) {
					this.recoveryQueue.add(job);
				} else {
					this.jobQueue.add(job);

				}
				assert (job.getFirstRMName() != null);
				ArrayList<RemoteSocket> receivers = new ArrayList<>();
				receivers.add(resourceManagerSockets.get(job.getFirstRMName()));
				AcknowledgementMessage cma = new AcknowledgementMessage(url, ack.getMessage());
				cma.setJobID(job.getId());
				sendMessage(receivers, cma);
			}
		} else if (ack.getMessage() instanceof JobMessage.RequestToDispatch) {
			RequestToDispatch message = ((JobMessage.RequestToDispatch) ack.getMessage());
			//If job is still in jobqueue remove and dispatch
			Job job = jobsWaitingForDispatch.get(message.getJob().getId());
			if (job != null) {
				ControlMessage addJobMessage = new JobMessage.AddJobMessage(url, message.getJob());
				Collection<RemoteSocket> receivers = new ArrayList<RemoteSocket>();
				receivers.add(resourceManagerSockets.get(message.getReceiver()));
				if(!sendMessage(receivers, addJobMessage)){
					resourceManagerLoad.put(message.getReceiver(), Float.MAX_VALUE);
				}else{
					jobsWaitingForDispatch.remove(message.getJob().getId());
					resourceManagerLoad.put(message.getReceiver(), resourceManagerLoad.get(message.getReceiver()) + (1.0f / resourceManagerNodeCounts.get(message.getReceiver())));
				}
			}

		}
	}

	private void saveToLog(JobMessage message) {
		Log logItem = new Log(message);
		LogList logItems = distributedLog.get(message.getSenderName());
		if (logItems == null) {
			logItems = new LogList();
		}
		logItems.addLogItem(logItem);
		distributedLog.put(message.getSenderName(), logItems);

	}

	private void sendRequestToAddJob(Job j) {
		RequestToAddJobMessage request = new RequestToAddJobMessage(url, j);
		sendMessage(gridSchedulerSockets.values(), request);
	}

	// finds the least loaded resource manager and returns its url
	private String getLeastLoadedRM() {
		String ret = null;
		float minLoad = Integer.MAX_VALUE;

		// loop over all resource managers, and pick the one with the lowest load
		for (String key : resourceManagerLoad.keySet()) {
			if (resourceManagerLoad.get(key) <= minLoad) {
				ret = key;
				minLoad = resourceManagerLoad.get(key);
			}
		}
		if (minLoad > 1.10) {
			return null;
		}
		return ret;
	}

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {
		while (true) {
			while (running) {
				// send a message to each resource manager, requesting its load
				ControlMessage cMessage = new ControlMessage.RequestLoadMessage(url);
				sendMessage(resourceManagerSockets.values(), cMessage);

				// schedule waiting messages to the different clusters
				while (!jobQueue.isEmpty() && running) {
					String leastLoadedRM = getLeastLoadedRM();
					System.out.println("Least loaded rm:" + leastLoadedRM);
					if (leastLoadedRM != null && resourceManagerSockets.get(leastLoadedRM) != null
							&& !rmStatus.get(leastLoadedRM).equals("Recovering")) {
						Job job = jobQueue.remove();
						job.setRequestTimeStamp(System.currentTimeMillis());
						job.setReceiverRM(leastLoadedRM);
						jobsWaitingForDispatch.put(job.getId(), job);
						ControlMessage requestToDispatch = new JobMessage.RequestToDispatch(url, job, leastLoadedRM);
						sendMessage(gridSchedulerSockets.values(), requestToDispatch);
					}
					for (Job j : jobsWaitingForDispatch.values()) {
						if (j.getRequestTimeStamp() + 4000 < System.currentTimeMillis()) {
							ControlMessage requestToDispatch = new JobMessage.RequestToDispatch(url, j,
									j.getReceiverRM());
							sendMessage(gridSchedulerSockets.values(), requestToDispatch);
						}
					}

				}
				// sleep
				try {
					Thread.sleep(pollSleep);
				} catch (InterruptedException ex) {
					assert (false) : "Grid scheduler runtread was interrupted";
				}
			}
		}
	}

	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopThreads() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert (false) : "Grid scheduler stopPollThread was interrupted while stopping pollingthread";
		}
		try {
			messageThread.join();
		} catch (InterruptedException ex) {
			assert (false) : "Grid scheduler stopPollThread was interrupted while stopping";
		}
	}

	private void addResourceManager(String url) {
		try {
			rmStatus.put(url, "ok");
			// RemoteSocket socket = (RemoteSocket) LocateRegistry.getRegistry(url.split("/")[2].split(":")[0], 5000).lookup(url);
			RemoteSocket socket = (RemoteSocket) Naming.lookup(url);
			resourceManagerSockets.put(url, socket);
			resourceManagerLoad.put(url, Float.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public LogList getLogForGS(String url) {
		return distributedLog.get(url);
	}

	private boolean sendMessage(Collection<RemoteSocket> receivers, Message message) {
		message.setSequenceID(messageSequenceId++);
		boolean out = true;
		for (RemoteSocket receiver : receivers) {
			if (receiver != null) {
				try {
					receiver.sendMessage(message);
				} catch (RemoteException e) {
					System.out.println("failed to send message");
					e.printStackTrace();
					out = false;
				}
			}
		}
		return out;
	}

	private void recoverOldLogRequest() {
		RequestLogMessage request = new RequestLogMessage(url, 1);
		sendMessage(gridSchedulerSockets.values(), request);
	}

	public void kill() {
		this.running = false;
		System.out.println("Killed GS: " + getURL());
	}

	public LinkedBlockingQueue<Message> getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(LinkedBlockingQueue<Message> queue) {
		this.messageQueue = queue;
	}

}
