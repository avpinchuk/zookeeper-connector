package cloud.connectors.zookeeper.ra;

public interface ServerDefs {

    String connectString = "localhost:2182";

    int port = 2182;

    int sessionTimeout = 3_000;

    long ttl = 50L;

}
