package ch.psi.daq.retrieval.config;

import ch.psi.daq.retrieval.utils.DateExt;

import java.time.ZoneOffset;

public class ChannelConfigEntry {

    @Override
    public String toString() {
        String dstr;
        try {
            dstr = DateExt.toInstant(ts).atZone(ZoneOffset.UTC).format(DateExt.datefmt);
        }
        catch (Exception e) {
            dstr = "fail " + e.toString();
        }
        return String.format("ChannelConfigEntry  %s  ts %20d  pulse %20d  ks %2d  bs %6d  sc %10d", dstr, ts, pulse, ks, bs, sc);
    }

    public long ts;
    public long pulse;
    public int ks;
    public long bs;
    public int sc;
    public byte dtFlags;
    public byte dtType;
    public boolean isCompressed;
    public boolean isArray;
    public boolean isBigEndian;
    public boolean hasShape;
    public byte dims;
    public int[] shape = new int[2];

}
