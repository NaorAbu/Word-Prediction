package ass2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class sortKeyComparator extends WritableComparator {
    protected sortKeyComparator() {
        super(sortKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    		sortKey k1 = (sortKey)w1;
    		sortKey k2 = (sortKey)w2;
         
        int result = k1.getW().compareTo(k2.getW());
        if(0 == result) {
            result = k1.compareTo(k2);
        }
        return result;
    }
}