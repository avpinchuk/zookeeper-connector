package cloud.connectors.zookeeper.ra.inbound;

import cloud.connectors.zookeeper.ra.AbstractZooKeeperTest;
import cloud.connectors.zookeeper.ra.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ejb.EJB;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@SuppressWarnings("ArquillianTooManyDeployment")
public class ZooKeeperSessionExpirationTest extends AbstractZooKeeperTest {

    @Deployment(name = "test", order = 2)
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                         .addClass(WatchedEventHandler.class)
                         .addClass(ExpiredMDB.class);
    }

    @BeforeClass
    @RunAsClient
    public static void setUpClass() throws Exception {
        zooKeeperServer = new ZooKeeperTestingServer(port);
        zooKeeperServer.start();
    }

    @AfterClass
    @RunAsClient
    public static void tearDownClass() throws IOException {
        zooKeeperServer.stop();
    }

    @Before
    @RunAsClient
    public void setUp() throws Exception {
        zooKeeperServer.stop();
        zooKeeperServer = new ZooKeeperTestingServer(port);
        zooKeeperServer.start();

        try (ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, null)) {
            Thread.sleep(sessionTimeout);   // fixme need more reliable condition than timeout
            zooKeeper.create("/expired", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @EJB
    private WatchedEventHandler eventHandler;

    @Test
    @OperateOnDeployment("test")
    public void testSessionExpiration() throws InterruptedException {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeCreated));
    }

}
