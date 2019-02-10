package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, AggregateIntFiled> groups; // <GroupBy, Aggregate> 
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		this.gbfield = gbfield;
    		this.gbfieldtype = gbfieldtype;
    		this.afield = afield;
    		this.what = what;
    		groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    		Field field = null;
    		if (gbfield != NO_GROUPING)
    			field = tup.getField(gbfield);
//    		else
//    			field = new IntField(Integer.MIN_VALUE);
    		
    		int val = ((IntField) tup.getField(this.afield)).getValue();
    		
//    		System.out.println("val: " + val);
//    		System.out.println(what);
    		if (groups.containsKey(field)) {
    			AggregateIntFiled aggregateIntFiled = groups.get(field);
            groups.put(field, aggregating(aggregateIntFiled, val));
    		}
    		else {
    			groups.put(field, new AggregateIntFiled(val, 1));
    		}
    }

    public AggregateIntFiled aggregating(AggregateIntFiled aggregateIntFiled, int newVal) {
//        System.out.println("aggregating");
    		aggregateIntFiled.count++;
    		switch (what) {
            case COUNT:
            		aggregateIntFiled.val = aggregateIntFiled.count;
                break;
            case MIN:
            		aggregateIntFiled.val = Math.min(aggregateIntFiled.val, newVal);
                break;
            case MAX:
            		aggregateIntFiled.val = Math.max(aggregateIntFiled.val, newVal);
                break;
            case SUM_COUNT:
            		break;
            case SUM:
            case AVG:
            		aggregateIntFiled.val += newVal;
                break;
            case SC_AVG:
            		break;
        }
//    		System.out.println(aggregateFiled);
        return aggregateIntFiled;
    }
    
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    		List<Tuple> aggregateTuples = new ArrayList<>();
    		
    		TupleDesc tupleDesc = getTupleDesc();
    		
        for(Field filed : groups.keySet()) {
            
        		AggregateIntFiled aggregateIntFiled = groups.get(filed);
            
            int val;
            if (what != Op.AVG)
            		val = aggregateIntFiled.val;
            else
            		val = aggregateIntFiled.val/aggregateIntFiled.count;
            
            Tuple tuple = new Tuple(tupleDesc);

            if (this.gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(val));
            } else {
                tuple.setField(0, filed);
                tuple.setField(1, new IntField(val));
            }
            aggregateTuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, aggregateTuples);
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    }
    
    public TupleDesc getTupleDesc() {
		TupleDesc tupleDesc;
		if (gbfield == NO_GROUPING)
			tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
		else
			tupleDesc = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
		return tupleDesc;
    }
}

class AggregateIntFiled {
	public int val;
	public int count;
	
	public AggregateIntFiled(int val, int count) {
		this.val = val;
		this.count = count;
	}
	
	public void countInc() {
		count++;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
	public String toString() {
		return "( " + val + ", " + count + ")";
	}
}
