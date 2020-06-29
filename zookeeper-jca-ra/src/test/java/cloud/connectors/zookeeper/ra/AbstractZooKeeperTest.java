package cloud.connectors.zookeeper.ra;

import cloud.connectors.zookeeper.api.OnZooKeeperEvent;
import cloud.connectors.zookeeper.api.ZooKeeperConnection;
import cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory;
import cloud.connectors.zookeeper.api.ZooKeeperListener;
import cloud.connectors.zookeeper.ra.inbound.ZooKeeperActivationSpec;
import cloud.connectors.zookeeper.ra.inbound.ZooKeeperResourceAdapter;
import cloud.connectors.zookeeper.ra.inbound.ZooKeeperWatcher;
import cloud.connectors.zookeeper.ra.outbound.ZooKeeperConnectionFactoryImpl;
import cloud.connectors.zookeeper.ra.outbound.ZooKeeperConnectionImpl;
import cloud.connectors.zookeeper.ra.outbound.ZooKeeperManagedConnection;
import cloud.connectors.zookeeper.ra.outbound.ZooKeeperManagedConnectionFactory;
import cloud.connectors.zookeeper.ra.outbound.ZooKeeperManagedConnectionMetadata;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.ScopeType;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependencies;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

@RunWith(Arquillian.class)
public abstract class AbstractZooKeeperTest {

    protected static final String connectString = "localhost:2182";

    protected static final int port = 2182;

    protected static final int sessionTimeout = 3_000;

    protected static final long ttl = 50L;

    protected static ZooKeeperTestingServer zooKeeperServer;

    @Rule
    public final TestName testName = new TestName();

    @Deployment(order = 1, testable = false)
    public static ResourceAdapterArchive createResourceAdapterDeployment() {
        // Resolve libraries from effective pom
        File[] libraries = Maven.resolver()
                                .loadPomFromFile("pom.xml")
                                .addDependencies(MavenDependencies.createDependency(
                                        "org.apache.zookeeper:zookeeper",
                                        ScopeType.COMPILE,
                                        false,
                                        MavenDependencies.createExclusion("org.slf4j:slf4j-log4j12"),
                                        MavenDependencies.createExclusion("log4j:log4j")))
                                .resolve()
                                .withTransitivity()
                                .asFile();
        // JCA API archive
        JavaArchive jcaApi = ShrinkWrap.create(JavaArchive.class, "zookeeper-jca-api.jar")
                                       .addClass(ZooKeeperListener.class)
                                       .addClass(OnZooKeeperEvent.class)
                                       .addClass(ZooKeeperConnection.class)
                                       .addClass(ZooKeeperConnectionFactory.class);
        // JCA implementation
        JavaArchive jcaRa = ShrinkWrap.create(JavaArchive.class, "zookeeper-jca-ra.jar")
                                      .addClass(ZooKeeperResourceAdapter.class)
                                      .addClass(ZooKeeperActivationSpec.class)
                                      .addClass(ZooKeeperWatcher.class)
                                      .addClass(ZooKeeperWatcher.class)
                                      .addClass(ZooKeeperConnectionImpl.class)
                                      .addClass(ZooKeeperConnectionFactoryImpl.class)
                                      .addClass(ZooKeeperManagedConnection.class)
                                      .addClass(ZooKeeperManagedConnectionFactory.class)
                                      .addClass(ZooKeeperManagedConnectionMetadata.class);
        // Resource adapter archive
        return ShrinkWrap.create(ResourceAdapterArchive.class, "zookeeper-rar.rar")
                         .addAsLibrary(jcaApi)
                         .addAsLibrary(jcaRa)
                         .addAsLibraries(libraries);
    }

}
