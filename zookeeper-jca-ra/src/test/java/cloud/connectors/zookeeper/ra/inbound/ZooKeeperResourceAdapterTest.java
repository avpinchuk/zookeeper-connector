package cloud.connectors.zookeeper.ra.inbound;

import cloud.connectors.zookeeper.ra.ResourceAdapterUtil;
import cloud.connectors.zookeeper.ra.ServerDefs;
import cloud.connectors.zookeeper.ra.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import javax.ejb.EJB;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Arquillian.class)
@SuppressWarnings("ArquillianTooManyDeployment")
public class ZooKeeperResourceAdapterTest {

    private static ZooKeeperTestingServer zooKeeperServer;

    @Rule
    public final TestName testName = new TestName();

    @Deployment(order = 1, testable = false)
    public static ResourceAdapterArchive createResourceAdapterDeployment() {
        return ResourceAdapterUtil.getResourceAdapter();
    }

    @Deployment(name = "test", order = 2)
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                         .addClass(WatchedEventHandler.class)
                         .addClass(RecursiveMDB.class)
                         .addClass(NonRecursiveMDB.class);
    }

    @BeforeClass
    @RunAsClient
    public static void setUpClass() throws Exception {
        zooKeeperServer = new ZooKeeperTestingServer(ServerDefs.port);
        zooKeeperServer.start();

        try (ZooKeeper zooKeeper = new ZooKeeper(ServerDefs.connectString, ServerDefs.sessionTimeout, null)){
            zooKeeper.create("/recursive", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create("/nonrecursive", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * We doesn't cleanup create nodes because they automatically go away after
     * server shutdown.
     *
     * @throws IOException if an error occurred
     */
    @AfterClass
    @RunAsClient
    public static void tearDownClass() throws IOException {
        zooKeeperServer.stop();
    }

    @Before
    @RunAsClient
    public void setUp() throws Exception {
        try (ZooKeeper zooKeeper = new ZooKeeper(ServerDefs.connectString, ServerDefs.sessionTimeout, null)) {
            switch (testName.getMethodName()) {
                case "testNodeCreatedRecursive":
                    zooKeeper.create("/recursive/node", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    break;
                case "testNodeDataChangedRecursive":
                    zooKeeper.setData("/recursive", "value".getBytes(), -1);
                    break;
                case "testNodeDeletedRecursive":
                    zooKeeper.delete("/recursive/node", -1);
                    break;
                case "testNodeCreatedNonRecursive":
                    zooKeeper.create("/nonrecursive/node", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    break;
                case "testNodeDataChangedNonRecursive":
                    zooKeeper.setData("/nonrecursive", "value".getBytes(), -1);
                    break;
                case "testNodeDeletedNonRecursive":
                    zooKeeper.delete("/nonrecursive/node", -1);
                    break;
                default:
                    break;
            }
        }
    }

    @EJB WatchedEventHandler eventHandler;

    @Test
    @OperateOnDeployment("test")
    public void testNodeCreatedRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeCreated));
    }

    @Test
    @OperateOnDeployment("test")
    public void testNodeDataChangedRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeDataChanged));
    }

    @Test
    @OperateOnDeployment("test")
    public void testNodeDeletedRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeDeleted));
    }

    @Test
    @OperateOnDeployment("test")
    public void testNodeCreatedNonRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeChildrenChanged));
    }

    @Test
    @OperateOnDeployment("test")
    public void testNodeDataChangedNonRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeDataChanged));
    }

    @Test
    @OperateOnDeployment("test")
    public void testNodeDeletedNonRecursive() throws Exception {
        assertThat(eventHandler.getEvent().getType(), is(Watcher.Event.EventType.NodeChildrenChanged));
    }

}
