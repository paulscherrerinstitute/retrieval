package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.UnpackScalarNumRes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;

public class AvgU64<T extends Num<T>> implements Avg<T> {

    public AvgU64(T num) {
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
        //node.set("avg", jfac.numberNode(Math.round((double) sum / count * 1e1) * 1e-1));
        return node;
    }

    @Override
    public void consider(UnpackScalarNumRes<T> res, int p1, int p2) {
        @SuppressWarnings("unchecked")
        UnpackScalarNumRes<U64> r2 = (UnpackScalarNumRes<U64>) res;
        AccU64 acc = (AccU64) r2.acc();
        int i = 0;
        for (Long k : acc.vals) {
            System.err.format("AvgU64 consider sees value  %d  %d\n", i, k);
            i += 1;
            if (count == 0) {
                min = k;
                max = k;
            }
            else {
                if (min >= 0 && k >= 0 || min < 0 && k < 0) {
                    min = Math.min(min, k);
                }
                else if (min < 0) {
                    min = k;
                }
                if (max >= 0 && k >= 0 || max < 0 && k < 0) {
                    max = Math.max(max, k);
                }
                else if (max >= 0) {
                    max = k;
                }
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
