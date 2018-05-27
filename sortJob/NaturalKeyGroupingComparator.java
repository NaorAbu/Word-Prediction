package ass2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(sortKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    		sortKey k1 = (sortKey)w1;
        sortKey k2 = (sortKey)w2;
         
        return k1.getW().compareTo(k2.getW());
    }
}