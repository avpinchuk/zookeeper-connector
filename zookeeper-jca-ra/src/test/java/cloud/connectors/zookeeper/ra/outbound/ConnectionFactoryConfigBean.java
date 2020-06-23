package cloud.connectors.zookeeper.ra.outbound;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.resource.ConnectionFactoryDefinition;

@Singleton
@Startup
@ConnectionFactoryDefinition(
    name = "java:comp/env/ZooKeeperConnectionFactory",
    interfaceName = "cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory",
    resourceAdapter = "zookeeper-rar",
    properties = {
            "connectString=localhost:2182"
    }
)
public class ConnectionFactoryConfigBean {

}
