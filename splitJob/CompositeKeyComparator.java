package ass2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(compositeKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    		compositeKey k1 = (compositeKey)w1;
    		compositeKey k2 = (compositeKey)w2;
         
        int result = k1.getW().compareTo(k2.getW());
        if(0 == result) {
            result = k1.compareTo(k2);
        }
        return result;
    }
}