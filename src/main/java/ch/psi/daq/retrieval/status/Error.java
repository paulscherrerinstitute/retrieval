package ch.psi.daq.retrieval.status;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class Error {

    public Error() {
        // json
    }

    public Error(String msg) {
        this.msg = msg;
    }

    public Error(Throwable e, String msg) {
        this.msg = msg;
        fillTrace(e);
    }

    public Error(Throwable e) {
        fillTrace(e);
    }

    void fillTrace(Throwable e) {
        msgException = e.getMessage();
        StackTraceElement[] tea = e.getStackTrace();
        if (tea != null) {
            bt = new ArrayList<>();
            for (StackTraceElement q : tea) {
                String h = q.toString();
                if (h.contains("daq.retrieval")) {
                    bt.add(q.toString());
                }
            }
        }
    }

    public String msg;
    public String msgException;
    public List<String> bt;

}
