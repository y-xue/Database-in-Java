package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private TupleDesc tupleDesc;
    private boolean isFetched;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    		this.transactionId = t;
    		this.child = child;
    		
    		Type[] types = new Type[] { Type.INT_TYPE };
        String[] fields = new String[] { null };
        tupleDesc = new TupleDesc(types, fields);
        isFetched = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    		child.open();
    		super.open();
    }

    public void close() {
        // some code goes here
    		super.close();
    		child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		child.rewind();
        isFetched = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		if (isFetched)
            return null;
        
        int count = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
            		Database.getBufferPool().deleteTuple(transactionId, tuple);
            } catch (TransactionAbortedException e) {
                throw new TransactionAbortedException();
            } catch (DbException e) {
            	throw new DbException("");
            } catch (IOException e) {
				// TODO: handle exception
			}
            count++;
        }

        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(count));
        isFetched = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		child = children[0];
    }

}
