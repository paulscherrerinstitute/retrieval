package ch.psi.daq.retrieval.error;

import org.springframework.scheduling.annotation.Scheduled;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class ErrorUtils {

    @Scheduled(fixedRate = 130000)
    public void cleaner() {
    }

    public static String traceString(Throwable e) {
        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(ba);
        e.printStackTrace(ps);
        ps.close();
        return ba.toString(StandardCharsets.UTF_8);
    }

}
