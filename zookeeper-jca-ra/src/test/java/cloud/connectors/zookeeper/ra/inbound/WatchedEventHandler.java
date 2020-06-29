package cloud.connectors.zookeeper.ra.inbound;

import org.apache.zookeeper.WatchedEvent;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Singleton
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class WatchedEventHandler {

    private final BlockingQueue<WatchedEvent> watchedEvents = new SynchronousQueue<>();

    public void setEvent(WatchedEvent event) throws InterruptedException {
        watchedEvents.put(event);
    }

    public WatchedEvent getEvent() throws InterruptedException {
        return watchedEvents.take();
    }

}
