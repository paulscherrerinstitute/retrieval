package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.UnpackScalarNumRes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;

public class AvgI64<T extends Num<T>> implements Avg<T> {

    public AvgI64(T num) {
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
        UnpackScalarNumRes<I64> r2 = (UnpackScalarNumRes<I64>) res;
        AccI64 acc = (AccI64) r2.acc();
        int i = 0;
        for (Long k : acc.vals) {
            i += 1;
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
        return String.format("count %d  min %d  max %d  sum %d", count, min, max, sum);
    }

    T num;
    long count;
    long min;
    long max;
    long sum;
    long last;

}
