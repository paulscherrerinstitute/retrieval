package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.Microseconds;

import java.util.List;

public class ScanDataFilesResult {
    public String channelName;
    public List<KeyspaceOrder2> keyspaces;
    public Microseconds duration;
}
