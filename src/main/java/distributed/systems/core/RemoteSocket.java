package distributed.systems.core;

import java.net.MalformedURLException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import distributed.systems.gridscheduler.model.Job;

public interface RemoteSocket extends Remote{

    public void sendMessage(Message message) throws RemoteException;

    public void addJob(Job job) throws RemoteException;

    public void register(String name) throws RemoteException, MalformedURLException;

}