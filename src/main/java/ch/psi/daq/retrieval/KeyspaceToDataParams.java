package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.finder.KeyspaceOrder2;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.time.Instant;
import java.util.List;

public class KeyspaceToDataParams {

    public KeyspaceToDataParams(KeyspaceOrder2 ks2, Instant begin, Instant end, List<Integer> splits, ReqCtx reqCtx, BufCtx bufCtx) {
        this.ks2 = ks2;
        this.begin = begin;
        this.end = end;
        this.splits = splits;
        this.reqCtx = reqCtx;
        this.bufCtx = bufCtx;
    }

    public KeyspaceOrder2 ks2;
    public Instant begin;
    public Instant end;
    public DataBufferFactory bufFac;
    public int bufferSize;
    public int filePrefetch;
    public int bufPrefetch;
    public List<Integer> splits;
    public ReqCtx reqCtx;
    public BufCtx bufCtx;

}
