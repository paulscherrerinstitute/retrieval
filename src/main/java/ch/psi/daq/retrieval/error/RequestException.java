package ch.psi.daq.retrieval.error;

import ch.psi.daq.retrieval.reqctx.ReqCtx;

public class RequestException extends RetrievalException {

    public RequestException(ReqCtx reqCtx, String msg) {
        super(reqCtx.toString() + "  " + msg);
    }

}
