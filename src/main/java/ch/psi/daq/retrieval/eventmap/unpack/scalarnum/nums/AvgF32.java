package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.UnpackScalarNumRes;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import org.slf4j.LoggerFactory;

public class AvgF32<T extends Num<T>> implements Avg<T> {

    public AvgF32(T num) {
        this.num = num;
    }

    @Override
    public JsonNode toJsonNode() {
        JsonNodeFactory jfac = JsonNodeFactory.instance;
        ObjectNode node = jfac.objectNode();
        node.set("count", jfac.numberNode(count));
        node.set("min", jfac.numberNode(min));
        node.set("max", jfac.numberNode(max));
        node.set("sum", jfac.numberNode(sum));
        if (count > 0) {
            node.set("avg", jfac.rawValueNode(new RawValue(String.format("%.3e", (double) sum / count))));
        }
        return node;
    }

    @Override
    public void consider(UnpackScalarNumRes<T> res, int p1, int p2) {
        @SuppressWarnings("unchecked")
        UnpackScalarNumRes<F32> r2 = (UnpackScalarNumRes<F32>) res;
        AccF32 acc = (AccF32) r2.acc();
        for (Float k : acc.vals) {
            if (count == 0) {
                min = k;
                max = k;
            }
            else {
                min = Math.min(min, k);
                max = Math.max(max, k);
            }
            sum += k;
            count += 1;
            last = k;
        }
    }

    @Override
    public void clear() {
        count = 0;
        min = 0;
        max = 0;
        sum = 0;
    }

    @Override
    public String finalString() {
        return String.format("count %d  min % .3e  max % .3e  sum % .3e", count, min, max, sum);
    }

    T num;
    long count;
    float min;
    float max;
    float sum;
    float last;

}
