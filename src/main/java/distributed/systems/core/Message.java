package distributed.systems.core;

import java.io.Serializable;


public interface Message extends Serializable{
    public void setSequenceID(long sequenceID);
    public long getSequenceID();
}