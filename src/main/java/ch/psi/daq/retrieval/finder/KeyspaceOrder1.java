package ch.psi.daq.retrieval.finder;

import java.util.List;

public class KeyspaceOrder1 {
    public String ksp;
    KeyspaceOrder1(String ksp) {
        this.ksp = ksp;
    }
    public List<BaseDirFinderFormatV0.TimeBin> timeBins;
}
