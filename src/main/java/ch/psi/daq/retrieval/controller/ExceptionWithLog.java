package ch.psi.daq.retrieval.controller;

public class ExceptionWithLog extends RuntimeException {

    public ExceptionWithLog(String msg, StringBuilder log) {
        super(msg);
        this.log = log;
    }

    StringBuilder log;

}
