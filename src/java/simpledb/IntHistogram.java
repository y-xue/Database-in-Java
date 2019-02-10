package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int min;
	private int max;
	private int buckets;
	private int nTups;
	private int width;
	private int[] histogram;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    		this.buckets = buckets;
    		this.min = min;
    		this.max = max;
    		
    		histogram = new int[buckets];
    		
    		double range = (double)(max - min + 1)/buckets;
    		width = (int)Math.ceil(range);
    }
    

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    		histogram[(v-min)/width]++;
    		nTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    		int index = (v-min) / width;
    		int right = v + width -1;
    		
    		double b_f;
    		double b_part;
    		double selectivity = 0;
    		
    		switch (op) {
			case EQUALS:
				if (v < min || v > max)
					return 0.0;
				else {
					int height = histogram[index];
					return (double) (height/width) / nTups;
				}
			case GREATER_THAN:
				if (v < min)
					return 1.0;
				if (v > max - 1)
					return 0.0;
	        		b_f = (double) histogram[index] / nTups;
	        		b_part = (double) (right-v) / width;
	        		selectivity = b_f * b_part;
	        		
	        		for (int i = index+1; i < histogram.length; i++)
	        			selectivity += (double) histogram[i] / nTups;
	        		
	        		return selectivity;
			case LESS_THAN:
				if (v <= min)
					return 0.0;
				if (v > max)
					return 1.0;
	        		
	        		for (int i = index-1; i >= 0; i--) 
	        			selectivity += (double) histogram[i] / nTups;
	        		
	        		return selectivity;
			case LESS_THAN_OR_EQ:
				if (v < min)
					return 0.0;
				if (v >= max)
					return 1.0;

	        		for (int i = index-1; i >= 0; i--)
	        			selectivity += (double) histogram[i] / nTups;
	        			
	        		selectivity += (double) (histogram[index ] /width) / nTups;
	        		return selectivity;
			case GREATER_THAN_OR_EQ:
				if (v <= min)
					return 1.0;
				if (v > max)
					return 0.0;

	        		b_f = (double) histogram[index] / nTups;
	        		b_part = (double) (right - v) / width;
	        		selectivity = b_f * b_part;
	        		
	        		for (int i = index+1; i < histogram.length; i++)
	        			selectivity += (double) histogram[i] / nTups;
	        			
	        		selectivity += (double) (histogram[index] / width) / nTups;
	        		return selectivity;
			case LIKE:
				if (v < min || v > max)
					return 0.0;
				return (double) (histogram[index] / width) / nTups;
			case NOT_EQUALS:
				if (v < min || v > max)
					return 1.0;
				return 1.0 - (double) (histogram[index] / width) / nTups;
			default:
				break;
		}
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
	    	String s = "";
	    	for (int i = 0; i < buckets; i++) {
	    		s += "bucket " + i + ": ";
	    		for (int j = 0; j < histogram[i]; j++) {
	    			s += "|";
	    		}
	    		s += "\n";
	    	}
        return s;
    }
}
