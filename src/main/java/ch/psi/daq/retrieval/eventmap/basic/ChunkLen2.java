package ch.psi.daq.retrieval.eventmap.basic;

public class ChunkLen2 implements Chunk {

    public ChunkLen2(int pos, int len) {
        this.pos = pos;
        this.len = len;
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
    public int bufIx;

}
