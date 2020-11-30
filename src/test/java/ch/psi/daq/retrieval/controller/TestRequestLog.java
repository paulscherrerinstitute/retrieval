package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.status.RequestStatus;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRequestLog {

    Map<String, RequestStatus> fillMap() {
        Map<String, RequestStatus> map = new HashMap<>();
        RequestStatus st;
        for (int i1 = 0; i1 < 10; i1 += 1) {
            st = new RequestStatus();
            st.timestamp = ZonedDateTime.parse("2020-02-01T00:00:00Z").plusMinutes(i1);
            map.put(String.format("req-%03d", i1), st);
        }
        return map;
    }

    @Test
    public void cleanToSize() {
        Map<String, RequestStatus> map = fillMap();
        QueryData.cleanGivenStatusLog(map, ZonedDateTime.parse("2020-02-01T00:01:00Z"), 2);
        assertThat(map.size()).isEqualTo(2);
    }

    @Test
    public void cleanToThreshold() {
        Map<String, RequestStatus> map = fillMap();
        QueryData.cleanGivenStatusLog(map, ZonedDateTime.parse("2020-02-01T00:05:00Z"), 20);
        assertThat(map.size()).isEqualTo(5);
    }

}
