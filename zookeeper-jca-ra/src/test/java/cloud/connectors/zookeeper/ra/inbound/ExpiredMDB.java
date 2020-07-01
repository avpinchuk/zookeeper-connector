package cloud.connectors.zookeeper.ra.inbound;

import cloud.connectors.zookeeper.api.OnZooKeeperEvent;
import cloud.connectors.zookeeper.api.ZooKeeperListener;
import org.apache.zookeeper.WatchedEvent;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;

@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "connectString", propertyValue = "localhost:2182"),
    @ActivationConfigProperty(propertyName = "sessionTimeout", propertyValue = "2000"),
    @ActivationConfigProperty(propertyName = "recursive", propertyValue = "true")
})
@SuppressWarnings("unused")
public class ExpiredMDB implements ZooKeeperListener {

    @EJB
    private WatchedEventHandler eventHandler;

    @OnZooKeeperEvent
    public void onEvent(WatchedEvent event) throws InterruptedException {
        eventHandler.setEvent(event);
    }

}
