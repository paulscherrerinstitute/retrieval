package ch.psi.daq.retrieval.finder;

public class LocatedChannel implements Comparable<LocatedChannel> {
    public BaseDir base;
    public String name;

    public LocatedChannel(BaseDir base, String name) {
        this.base = base;
        this.name = name;
    }
    @Override
    public int compareTo(LocatedChannel x) {
        int n = base.compareTo(x.base);
        if (n != 0) {
            return n;
        }
        else {
            return name.compareTo(x.name);
        }
    }
    @Override
    public String toString() {
        return String.format("LocatedChannel { base: %s, name: %s }", base, name);
    }
}
