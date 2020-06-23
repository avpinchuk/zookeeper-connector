package cloud.connectors.zookeeper.ra.outbound;

import org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;

class ComparableStat implements Comparable<ComparableStat> {

    private final Stat stat;

    ComparableStat() {
        this.stat = new Stat();
    }

    @Override
    public int compareTo(@Nonnull ComparableStat other) {
        return stat.compareTo(other.stat);
    }

    public Stat stat() {
        return stat;
    }

}
