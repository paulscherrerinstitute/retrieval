package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.AggResult;
import ch.psi.daq.retrieval.pod.api1.AggResultNothing;
import ch.psi.daq.retrieval.pod.api1.AggResultSum;
import com.fasterxml.jackson.databind.JsonNode;

public class AggMax implements AggFunc {
    double max = Double.NEGATIVE_INFINITY;

    public String name() { return "max"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            if (v > max) {
                max = v;
            }
        }
    }

    public void reset() {
    }

    public AggResult result() {
        if (max == Double.NEGATIVE_INFINITY) {
            return new AggResultNothing();
        }
        AggResultSum ret = new AggResultSum();
        ret.sum = max;
        return ret;
    }

}
