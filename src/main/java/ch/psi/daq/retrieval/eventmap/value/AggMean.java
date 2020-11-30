package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.AggResult;
import ch.psi.daq.retrieval.pod.api1.AggResultSum;
import com.fasterxml.jackson.databind.JsonNode;

public class AggMean implements AggFunc {
    double sum = 0.0;
    long n = 0;

    public String name() { return "mean"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            sum += v;
            n += 1;
        }
    }

    public void reset() {
        sum = 0.0;
        n = 0;
    }

    public AggResult result() {
        AggResultSum ret = new AggResultSum();
        ret.sum = sum / n;
        return ret;
    }

}
