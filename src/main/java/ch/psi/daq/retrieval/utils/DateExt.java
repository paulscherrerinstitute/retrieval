package ch.psi.daq.retrieval.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateExt {

    public static Instant toInstant(long k) {
        return Instant.ofEpochSecond(k / 1000000000L).plus(Duration.ofNanos(k % 1000000000L));
    }

    public static ZonedDateTime toZoned(long k) {
        return toInstant(k).atZone(ZoneOffset.UTC);
    }

    public static String toString(long k) {
        return toZoned(k).format(datefmt);
    }

    public static long toLong(Instant k) {
        return k.getEpochSecond() * 1000000000L + k.getNano();
    }

    public static ZonedDateTime utcFromNanos(long k) {
        Instant i = Instant.ofEpochSecond(k / 1000000000L, k % 1000000000L);
        return i.atZone(ZoneOffset.UTC);
    }

    public static final DateTimeFormatter datefmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSz");

}
