package distributed.systems.core;


import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import distributed.systems.gridscheduler.model.Job;

public class Socket extends UnicastRemoteObject implements RemoteSocket {

    static final long serialVersionUID = -234435345;
    private IMessageReceivedHandler messageReceivedHandler; 
    private IAddJobHandler jobHandler;

    public Socket() throws RemoteException {
        super();
    }
    
    public void sendMessage(Message message) throws RemoteException {
        messageReceivedHandler.onMessageReceived(message);
    }
    
    public void addJob(Job job) throws RemoteException {
        jobHandler.addJob(job);
    }

    public void register(String name) throws RemoteException, MalformedURLException {
        // LocateRegistry.getRegistry(name.split("/")[2].split(":")[0], 5000).rebind(name, this); 
        Naming.rebind(name, this);
    }

    public void addJobHandler(IAddJobHandler handler) {
        this.jobHandler = handler;
    }

    public void addMessageReceivedHandler(IMessageReceivedHandler handler)   {
        this.messageReceivedHandler = handler;
    }
    
}