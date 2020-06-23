package cloud.connectors.zookeeper.ra.outbound;

import cloud.connectors.zookeeper.api.ZooKeeperConnection;
import cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Resource;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.resource.ResourceException;
import java.util.List;

@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ZooKeeperConnectionBean implements ZooKeeperConnection {

    @Resource(name = "java:comp/env/ZooKeeperConnectionFactory")
    private ZooKeeperConnectionFactory connectionFactory;

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.create(path, data, acl, createMode);
        }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.create(path, data, acl, createMode, stat);
        }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.create(path, data, acl, createMode, stat, ttl);
        }
    }

    @Override
    public void delete(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            connection.delete(path);
        }
    }

    @Override
    public void delete(String path, int version) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            connection.delete(path, version);
        }
    }

    @Override
    public Stat exists(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.exists(path);
        }
    }

    @Override
    public List<ACL> getACL(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getACL(path);
        }
    }

    @Override
    public List<ACL> getACL(String path, Stat stat) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getACL(path, stat);
        }
    }

    @Override
    public int getAllChildrenNumber(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getAllChildrenNumber(path);
        }
    }

    @Override
    public List<String> getChildren(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getChildren(path);
        }
    }

    @Override
    public List<String> getChildren(String path, Stat stat) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getChildren(path, stat);
        }
    }

    @Override
    public byte[] getData(String path) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getData(path);
        }
    }

    @Override
    public byte[] getData(String path, Stat stat) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getData(path, stat);
        }
    }

    @Override
    public List<String> getEphemerals() throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getEphemerals();
        }
    }

    @Override
    public List<String> getEphemerals(String prefixPath) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.getEphemerals(prefixPath);
        }
    }

    @Override
    public Stat setACL(String path, List<ACL> acl) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.setACL(path, acl);
        }
    }

    @Override
    public Stat setACL(String path, List<ACL> acl, int version) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.setACL(path, acl, version);
        }
    }

    @Override
    public Stat setData(String path, byte[] data) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.setData(path, data);
        }
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws ResourceException {
        try (ZooKeeperConnection connection = connectionFactory.getConnection()) {
            return connection.setData(path, data, version);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

}
