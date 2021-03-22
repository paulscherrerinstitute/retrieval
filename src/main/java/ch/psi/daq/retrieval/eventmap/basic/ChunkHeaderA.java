package ch.psi.daq.retrieval.eventmap.basic;

public class ChunkHeaderA implements Chunk {

    public ChunkHeaderA(int pos, int len, long ts, long pulse) {
        this.pos = pos;
        this.len = len;
        this.ts = ts;
        this.pulse = pulse;
    }

    @Override
    public void setBufIx(int bufIx) {
        this.bufIx = bufIx;
    }

    @Override
    public String desc() {
        return getClass().getSimpleName();
    }

    public int pos;
    public int len;
    public long ts;
    public long pulse;
    public int bufIx;

}
