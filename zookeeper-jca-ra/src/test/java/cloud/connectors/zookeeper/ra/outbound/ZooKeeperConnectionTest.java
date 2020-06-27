package cloud.connectors.zookeeper.ra.outbound;

import cloud.connectors.zookeeper.ra.ResourceAdapterUtil;
import cloud.connectors.zookeeper.ra.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import javax.ejb.EJB;
import javax.resource.ResourceException;
import javax.resource.spi.EISSystemException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThrows;

@RunWith(Arquillian.class)
@SuppressWarnings("ArquillianTooManyDeployment")
public class ZooKeeperConnectionTest {

    private static ZooKeeperTestingServer zooKeeperServer;

    private static final long ttl = 50L;

    private static final String connectString = "localhost:2182";

    private static final int port = 2182;

    private static final int sessionTimeout = 3000;

    private final List<String> nodes = new ArrayList<>();

    private final ComparableStat emptyStat = new ComparableStat();

    private final EnumSet<CreateMode> createModes = EnumSet.allOf(CreateMode.class);

    @Rule
    public final TestName testName = new TestName();


    @Deployment(order = 1, testable = false)
    public static ResourceAdapterArchive createResourceAdapterDeployment() {
        return ResourceAdapterUtil.getResourceAdapter();
    }

    @Deployment(name = "test", order = 2)
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                         .addClass(ConnectionFactoryConfigBean.class)
                         .addClass(ZooKeeperConnectionBean.class);
    }

    @BeforeClass
    @RunAsClient
    public static void setUpClass() throws Exception {
        zooKeeperServer = new ZooKeeperTestingServer(port);
        zooKeeperServer.start();
    }

    @AfterClass
    @RunAsClient
    public static void tearDownClass() throws Exception {
        zooKeeperServer.stop();
    }

    @Before
    @RunAsClient
    public void setUp() throws Exception {
        ZooKeeper zooKeeper;
        switch (testName.getMethodName()) {
            case "testDelete":
                zooKeeper = new ZooKeeper(connectString, sessionTimeout, null);
                zooKeeper.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.close();
                break;
            case "testDeleteVersion":
                zooKeeper = new ZooKeeper(connectString, sessionTimeout, null);
                zooKeeper.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.create("/node1", "value1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.close();
                break;
            case "testExists":
            case "testGetACL":
            case "testGetACLStat":
            case "testGetData":
            case "testGetDataStat":
            case "testSetACL":
            case "testSetACLVersion":
            case "testSetData":
            case "testSetDataVersion":
                zooKeeper = new ZooKeeper(connectString, sessionTimeout, null);
                nodes.add(zooKeeper.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                zooKeeper.close();
                break;
            case "testGetAllChildrenNumber":
            case "testGetChildren":
            case "testGetChildrenStat":
                zooKeeper = new ZooKeeper(connectString, sessionTimeout, null);
                nodes.add(zooKeeper.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                zooKeeper.create("/node0/node00", "value00".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.create("/node0/node01", "value01".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.close();
                break;
            default:
                break;
        }

    }

    @After
    @RunAsClient
    public void tearDown() throws Exception {
        if (!nodes.isEmpty()) {
            ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, null);
            nodes.forEach(node -> {
                try {
                    // we doesn't create zk tree deeper than 2
                    // (parent and one children node)
                    ZKUtil.deleteRecursive(zooKeeper, node, 2);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            nodes.clear();
            zooKeeper.close();
        }
    }

    @EJB
    private ZooKeeperConnectionBean connection;

    @Test
    @OperateOnDeployment("test")
    public void testCreate() throws ResourceException {
        int i = 0;
        for (CreateMode createMode : createModes) {
            if (createMode.isTTL()) {
                continue;
            }

            String path = "/node" + i;
            byte[] data = ("value" + i).getBytes();

            String actualPath = connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            nodes.add(actualPath);

            // NodeExists error code
            if (createMode.isSequential()) {
                // sequential nodes does not returns NodeExists condition
                nodes.add(connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode));
            } else {
                assertThat(assertThrows(EISSystemException.class,
                                        () -> connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode)).getCause(),
                           instanceOf(KeeperException.NodeExistsException.class));
            }

            // create children
            if (createMode.isEphemeral()) {
                // ephemeral nodes cannot contains children
                assertThat(assertThrows(EISSystemException.class,
                                        () -> connection.create(actualPath + path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode)).getCause(),
                           instanceOf(KeeperException.NoChildrenForEphemeralsException.class));
            } else {
                connection.create(actualPath + path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }
            i++;
        }
    }

    @Test
    @OperateOnDeployment("test")
    public void testCreateStat() throws ResourceException {
        int i = 0;
        for (CreateMode createMode : createModes) {
            if (createMode.isTTL()) {
                continue;
            }

            String path = "/node" + i;
            byte[] data = ("value" + i).getBytes();

            ComparableStat stat = new ComparableStat();
            String actualPath = connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat.stat());
            nodes.add(actualPath);
            assertThat(emptyStat, lessThan(stat));

            // NodeExists error code
            ComparableStat stat2 = new ComparableStat();
            if (createMode.isSequential()) {
                // sequential nodes does not returns NodeExists condition
                nodes.add(connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat2.stat()));
                assertThat(stat, lessThan(stat2));
            } else {
                assertThat(assertThrows(EISSystemException.class,
                                        () -> connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat2.stat())).getCause(),
                           instanceOf(KeeperException.NodeExistsException.class));
            }

            // create children
            ComparableStat stat3 = new ComparableStat();
            if (createMode.isEphemeral()) {
                // ephemeral nodes cannot contains children
                assertThat(assertThrows(EISSystemException.class,
                                        () -> connection.create(actualPath + path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat3.stat())).getCause(),
                           instanceOf(KeeperException.NoChildrenForEphemeralsException.class));
            } else {
                connection.create(actualPath + path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat3.stat());
                assertThat(stat2, lessThan(stat3));
            }
            i++;
        }
    }

    @Test
    @OperateOnDeployment("test")
    public void testCreateTTL() throws ResourceException {
        System.setProperty("zookeeper.extendedTypesEnabled", "true");

        int i = 0;
        for (CreateMode createMode : createModes) {
            if (!createMode.isTTL()) {
                continue;
            }

            String path = "/node" + i;
            byte[] data = ("value" + i).getBytes();

            ComparableStat stat = new ComparableStat();
            nodes.add(connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat.stat(), ttl));
            assertThat(emptyStat, lessThan(stat));

            ComparableStat stat2 = new ComparableStat();
            if (createMode.isSequential()) {
                nodes.add(connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat2.stat(), ttl));
                assertThat(stat, lessThan(stat2));
            } else {
                assertThat(assertThrows(EISSystemException.class,
                                        () -> connection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, stat2.stat(), ttl)).getCause(),
                           instanceOf(KeeperException.NodeExistsException.class));
            }
            i++;
        }
    }

    @Test
    @OperateOnDeployment("test")
    public void testDelete() throws ResourceException {
        connection.delete("/node0");
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.delete("/node0")).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testDeleteVersion() throws ResourceException {
        connection.delete("/node0", 0);
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.delete("/node0", 0)).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.delete("/node1", 1)).getCause(),
                   instanceOf(KeeperException.BadVersionException.class));

        connection.delete("/node1", -1);
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.delete("/node1", -1)).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testExists() throws ResourceException {
        assertThat(connection.exists("/node0"), notNullValue());
        assertThat(connection.exists("/node1"), nullValue());
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetACL() throws ResourceException {
        assertThat(connection.getACL("/node0"), is(ZooDefs.Ids.OPEN_ACL_UNSAFE));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getACL("/node1")).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetACLStat() throws ResourceException {
        ComparableStat stat = new ComparableStat();
        assertThat(connection.getACL("/node0", stat.stat()), is(ZooDefs.Ids.OPEN_ACL_UNSAFE));
        assertThat(emptyStat, lessThan(stat));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getACL("/node1", stat.stat())).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetAllChildrenNumber() throws ResourceException {
        assertThat(connection.getAllChildrenNumber("/node0"), is(2));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getAllChildrenNumber("/node1")).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetChildren() throws ResourceException {
        assertThat(connection.getChildren("/node0").size(), is(2));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getChildren("/node1")).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetChildrenStat() throws ResourceException {
        ComparableStat stat = new ComparableStat();
        assertThat(connection.getChildren("/node0", stat.stat()).size(), is(2));
        assertThat(emptyStat, lessThan(stat));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getChildren("/node1", stat.stat())).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetData() throws ResourceException {
        assertThat(connection.getData("/node0"), is("value0".getBytes()));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getData("/node1")).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetDataStat() throws ResourceException {
        ComparableStat stat = new ComparableStat();
        assertThat(connection.getData("/node0", stat.stat()), is("value0".getBytes()));
        assertThat(emptyStat, lessThan(stat));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.getData("/node1", stat.stat())).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetEphemerals() throws ResourceException {
        connection.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        assertThat(connection.getEphemerals().size(), is(1));
        connection.delete("/node0");
    }

    @Test
    @OperateOnDeployment("test")
    public void testGetEphemeralsPrefix() throws ResourceException {
        connection.create("/node0", "value0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        connection.create("/node1", "value1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        assertThat(connection.getEphemerals("/node").size(), is(2));
        assertThat(connection.getEphemerals("/notfound").size(), is(0));

        connection.delete("/node0");
        connection.delete("/node1");
    }

    @Test
    @OperateOnDeployment("test")
    public void testSetACL() throws ResourceException {
        Stat stat = connection.setACL("/node0", ZooDefs.Ids.OPEN_ACL_UNSAFE);
        assertThat(stat.getAversion(), is(1));
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setACL("/node1", ZooDefs.Ids.OPEN_ACL_UNSAFE)).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testSetACLVersion() throws ResourceException {
        Stat stat = connection.setACL("/node0", ZooDefs.Ids.OPEN_ACL_UNSAFE, 0);
        assertThat(stat.getAversion(), is(1));

        stat = connection.setACL("/node0", ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
        assertThat(stat.getAversion(), is(2));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setACL("/node0", ZooDefs.Ids.OPEN_ACL_UNSAFE, 3)).getCause(),
                   instanceOf(KeeperException.BadVersionException.class));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setACL("/node1", ZooDefs.Ids.OPEN_ACL_UNSAFE, 0)).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testSetData() throws ResourceException {
        Stat stat = connection.setData("/node0", "value0".getBytes());
        assertThat(stat.getVersion(), is(1));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setData("/node1", "value1".getBytes())).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));

        byte[] data = new byte[2 * 1024 * 1024];
        Arrays.fill(data, (byte) 'a');
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setData("/node0", data)).getCause(),
                   instanceOf(KeeperException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testSetDataVersion() throws ResourceException {
        Stat stat = connection.setData("/node0", "value0".getBytes(), 0);
        assertThat(stat.getVersion(), is(1));

        stat = connection.setData("/node0", "value0".getBytes(), -1);
        assertThat(stat.getVersion(), is(2));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setData("/node0", "value0".getBytes(), 3)).getCause(),
                   instanceOf(KeeperException.BadVersionException.class));

        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setData("/node1", "value1".getBytes(), 0)).getCause(),
                   instanceOf(KeeperException.NoNodeException.class));

        byte[] data = new byte[2 * 1024 * 1024];
        Arrays.fill(data, (byte) 'a');
        assertThat(assertThrows(EISSystemException.class,
                                () -> connection.setData("/node0", data, -1)).getCause(),
                   instanceOf(KeeperException.class));
    }

    @Test
    @OperateOnDeployment("test")
    public void testClose() {
        connection.close();
    }

}
