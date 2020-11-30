package ch.psi.daq.retrieval.finder;

import java.util.ArrayList;
import java.util.List;

public class Split {
    public int split;
    public List<TimeBin2> timeBins;
    public Split(int split) {
        this.split = split;
        this.timeBins = new ArrayList<>();
    }
}
