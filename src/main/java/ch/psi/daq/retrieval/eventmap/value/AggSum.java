package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.AggResult;
import ch.psi.daq.retrieval.pod.api1.AggResultSum;
import com.fasterxml.jackson.databind.JsonNode;

public class AggSum implements AggFunc {
    double sum = 0.0;

    public String name() { return "sum"; }

    public void sink(JsonNode node) {
        //LOGGER.warn("sinking value {}", node);
        if (node.isNumber()) {
            //LOGGER.warn("sinking number {}", node.asDouble());
            double v = node.asDouble();
            sum += v;
        }
    }

    public void reset() {
        sum = 0.0;
    }

    public AggResult result() {
        AggResultSum ret = new AggResultSum();
        ret.sum = sum;
        return ret;
    }

}
