package ch.psi.daq.retrieval.error;

public class MessageException extends RuntimeException {

    public MessageException(String msg) {
        super(msg, null, false, false);
    }

    public MessageException(String msg, Throwable e) {
        super(msg, e, false, false);
    }

}
