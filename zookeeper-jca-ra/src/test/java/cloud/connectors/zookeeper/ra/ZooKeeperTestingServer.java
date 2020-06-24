package cloud.connectors.zookeeper.ra;

import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public final class ZooKeeperTestingServer {

    private final int port;

    private TestingServer zooKeeperServer;

    public ZooKeeperTestingServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        Path tmpDir = Files.createTempDirectory("zookeeper");
        // cleanup temp directory hook
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                try {
                    Files.walk(tmpDir)
                         .sorted(Comparator.reverseOrder())
                         .forEach(path -> {
                             try {
                                 Files.delete(path);
                             } catch (IOException ignore) { }
                         });
                } catch (IOException ignore) { }
            })
        );
        zooKeeperServer = new TestingServer(port, tmpDir.toFile());
    }

    public void stop() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.stop();
        }
    }

}
