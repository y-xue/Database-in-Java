package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean isFetched;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    		this.transactionId = t;
    		this.child = child;
    		this.tableId = tableId;
    		
    		HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        if (!this.child.getTupleDesc().equals(heapFile.getTupleDesc())) {
            throw new DbException("Tuple descriptor mismatch");
        }

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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		if (isFetched)
            return null;

        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, t);
            } catch (IOException e) {
                throw new TransactionAbortedException();
            }
            count++;
        }

        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(count));
        isFetched=true;
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
