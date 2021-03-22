package ch.psi.daq.retrieval.status;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRequestLog {

    RequestStatusBoard makeFilledBoard() {
        // TODO rsb parameter
        RequestStatusBoard board = new RequestStatusBoard(null);
        for (int i1 = 0; i1 < 10; i1 += 1) {
            ZonedDateTime dt = ZonedDateTime.parse("2020-02-01T00:00:00Z").plusMinutes(i1);
            RequestStatus st = new RequestStatus();
            st.tsl = dt;
            board.map.put(String.format("req-%03d", i1), st);
        }
        return board;
    }

    @Test
    public void cleanToSize() {
        RequestStatusBoard board = makeFilledBoard();
        board.clean(ZonedDateTime.parse("2020-02-01T00:01:00Z"), 2);
        assertThat(board.map.size()).isEqualTo(2);
    }

    @Test
    public void cleanToThreshold() {
        RequestStatusBoard board = makeFilledBoard();
        board.clean(ZonedDateTime.parse("2020-02-01T00:05:00Z"), 20);
        assertThat(board.map.size()).isEqualTo(5);
    }

}
