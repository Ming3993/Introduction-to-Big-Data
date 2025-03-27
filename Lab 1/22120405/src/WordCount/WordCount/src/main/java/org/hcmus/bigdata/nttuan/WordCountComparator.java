package org.hcmus.bigdata.nttuan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Map;

/// This class makes the keys that Receivers receive follow the order as: a, f, j, g, h, c, m, u, s
/// Hence, the output follows this order also.
public class WordCountComparator extends WritableComparator {
    protected WordCountComparator() {
        super(Text.class, true);
    }

    // Create a map of orders
    private static final Map<String, Integer> mapOrder = Map.of(
            "a", 1,
            "f", 2,
            "j", 3,
            "g", 4,
            "h", 5,
            "c", 6,
            "m", 7,
            "u", 8,
            "s", 9
    );

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Integer order1 = mapOrder.get(w1.toString());
        Integer order2 = mapOrder.get(w2.toString());
        return Integer.compare(order1, order2);
    }
}
