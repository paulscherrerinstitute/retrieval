package ch.psi.daq.retrieval.controller;

public class LogRes<T> {

    public void println(String format, Object... args) {
        sb.append(String.format(format, args)).append('\n');
    }

    public synchronized void append(LogRes<?> k) {
        sb.append(k.sb);
    }

    public Exception toException(String msg) {
        return new ExceptionWithLog(msg, sb);
    }

    StringBuilder sb = new StringBuilder();

}
