package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private HashMap<Field, AggregateStringFiled> groups; 
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		if (what != Op.COUNT)
    			throw new IllegalArgumentException();
    		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    		Field field = null;
		if (gbfield != NO_GROUPING)
			field = tup.getField(gbfield);
		
		String val = ((StringField) tup.getField(this.afield)).getValue();
		
		if (groups.containsKey(field)) {
			AggregateStringFiled aggregateStringFiled = groups.get(field);
			groups.put(field, aggregating(field, aggregateStringFiled, val));
		}
		else {
			groups.put(field, new AggregateStringFiled(val, 1));
		}
    }

    public AggregateStringFiled aggregating(Field field, AggregateStringFiled aggregateStringFiled, String newVal) {
    		aggregateStringFiled.count++;
  		return aggregateStringFiled;
    }
    
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    		List<Tuple> aggregateTuples = new ArrayList<>();
		
		TupleDesc tupleDesc = getTupleDesc();
				
		for(Field filed : groups.keySet()) {
        
			AggregateStringFiled aggregateStringFiled = groups.get(filed);
        
			int count = aggregateStringFiled.count;
        
			Tuple tuple = new Tuple(tupleDesc);

	        if (this.gbfield == NO_GROUPING) {
	            tuple.setField(0, new IntField(count));
	        } else {
	            tuple.setField(0, filed);
	            tuple.setField(1, new IntField(count));
	        }
	        aggregateTuples.add(tuple);
		}
		return new TupleIterator(tupleDesc, aggregateTuples);
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

class AggregateStringFiled {
	public String val;
	public int count;
	
	public AggregateStringFiled(String val, int count) {
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
