package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private List<TDItem> tDItemList;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        
        public Type getFieldType() {
        		return this.fieldType;
        }
        
        public String getFieldName() {
    			return this.fieldName;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tDItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    		this.tDItemList = new ArrayList<>(typeAr.length);
    		for (int i = 0; i < typeAr.length; i++) {
    			this.tDItemList.add(new TDItem(typeAr[i],fieldAr[i]));
    		}
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    		this.tDItemList = new ArrayList<>(typeAr.length);
		for (int i = 0; i < typeAr.length; i++) {
			this.tDItemList.add(new TDItem(typeAr[i], null));
		}
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc tDItemList.
     * 
     * @param tDItemList
     *            List specifying the information of each field.
     */
    public TupleDesc(List<TDItem> tDItemList) {
            this.tDItemList = tDItemList;
        // some code goes here
    }

    /**
     * @return the list of TDItem in this TupleDesc
     */
    public List<TDItem> getTDItems() {
        // some code goes here
        return this.tDItemList;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tDItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.tDItemList.get(i).getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    		if (i >= this.tDItemList.size()) {
    			throw new NoSuchElementException();
    		}
        return this.tDItemList.get(i).getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    		int idx = -1;
    		if (name == null) {
    			for (int i = 0; i < this.tDItemList.size(); i++) {
        			if (this.tDItemList.get(i).getFieldName() == null) {
        				idx = i;
        				break;
        			}
        		}
    		}
    		else {
	    		for (int i = 0; i < this.tDItemList.size(); i++) {
	    			if (name.equals(this.tDItemList.get(i).getFieldName())) {
	    				idx = i;
	    				break;
	    			}
	    		}
    		}
    		if (idx == -1) {
    			throw new NoSuchElementException();
    		}
    		return idx;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    		int size = 0;
    		for (TDItem tDItem : tDItemList) {
    			size += tDItem.getFieldType().getLen();
    		}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
//    		TupleDesc mergedTD = new TupleDesc();
//    		System.out.println(td1);
//    		System.out.println(td2);
    		List<TDItem> tDItemList = new ArrayList<>(td1.getTDItems());
    		tDItemList.addAll(td2.getTDItems());
        return new TupleDesc(tDItemList);
    }
    
    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    		if (!(o instanceof TupleDesc)) {
    			return false;
    		}
    		TupleDesc tTD = (TupleDesc) o;
        int numOfFields = this.numFields();
        if (numOfFields != tTD.numFields()) {
            return false;
        }
        for (int i = 0; i < numOfFields; i++) {
            if (this.getFieldType(i) != tTD.getFieldType(i)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String s = "";
        for (TDItem tDItem : tDItemList) {
            s += tDItem.getFieldType() + "(" + tDItem.getFieldName() + ")" + ",";
        }
        // some code goes here
        return s.substring(0, s.length() - 1);
    }
}
