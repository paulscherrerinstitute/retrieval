package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.bytes.BufCont;

public interface Markable {

    void markWith(BufCont.Mark mark);

}
