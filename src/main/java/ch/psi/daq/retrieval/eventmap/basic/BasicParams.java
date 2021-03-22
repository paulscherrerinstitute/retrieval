package ch.psi.daq.retrieval.eventmap.basic;

import ch.psi.daq.retrieval.reqctx.BufCtx;

public class BasicParams {
    public BasicParams(BufCtx bufCtx, long endNanos, String name) {
        this.bufCtx = bufCtx;
        this.endNanos = endNanos;
        this.name = name;
    }
    long endNanos;
    BufCtx bufCtx;
    String name;
}
