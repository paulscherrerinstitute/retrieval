package ch.psi.daq.retrieval.finder;

import java.nio.file.Path;
import java.util.List;

public class KeyspaceOrder2 {
    public LocatedChannel locatedChannel;
    public String ksp;
    public KeyspaceOrder2(String ksp) {
        this.ksp = ksp;
    }
    public List<Split> splits;

    public Path filePath(TimeBin2 tb, Split sp) {
        String p = String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data", locatedChannel.base.baseDir, locatedChannel.base.baseKeyspaceName, ksp, locatedChannel.name, tb.timeBin, sp.split, tb.binSize, 0);
        return Path.of(p);
    }

}
