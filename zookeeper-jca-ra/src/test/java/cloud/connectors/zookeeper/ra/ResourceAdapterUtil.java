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
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.ScopeType;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependencies;

import java.io.File;

public final class ResourceAdapterUtil {

    private ResourceAdapterUtil() {
        throw new AssertionError();
    }

    public static ResourceAdapterArchive getResourceAdapter() {
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
        JavaArchive jcaRa = ShrinkWrap.create(JavaArchive.class, "zookeeper-jaca-ra.jar")
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
