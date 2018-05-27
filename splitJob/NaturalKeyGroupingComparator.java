package ass2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(compositeKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    		compositeKey k1 = (compositeKey)w1;
        compositeKey k2 = (compositeKey)w2;
         
        return k1.getW().compareTo(k2.getW());
    }
}