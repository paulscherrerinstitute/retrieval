package ch.psi.daq.retrieval.finder;

import java.util.List;

public class KeyspaceOrder2 {
    public Channel channel;
    public String ksp;
    public KeyspaceOrder2(String ksp) {
        this.ksp = ksp;
    }
    public List<Split> splits;
}
