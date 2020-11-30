package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.AggResult;
import ch.psi.daq.retrieval.pod.api1.AggResultNothing;
import ch.psi.daq.retrieval.pod.api1.AggResultSum;
import com.fasterxml.jackson.databind.JsonNode;

public class AggMin implements AggFunc {
    double min = Double.POSITIVE_INFINITY;

    public String name() { return "min"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            if (v < min) {
                min = v;
            }
        }
    }

    public void reset() {
    }

    public AggResult result() {
        if (min == Double.POSITIVE_INFINITY) {
            return new AggResultNothing();
        }
        AggResultSum ret = new AggResultSum();
        ret.sum = min;
        return ret;
    }

}
