package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    		this.file = f;
    		this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    		return this.file.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    		return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    		HeapPage heapPage;
    		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
			int pageSize = BufferPool.getPageSize();
			int pageNumber = pid.getPageNumber();
			int offset = pageNumber * pageSize;
			byte[] data = new byte[pageSize];
			randomAccessFile.seek(offset);
			randomAccessFile.read(data, 0, pageSize);
			randomAccessFile.close();
			heapPage = new HeapPage((HeapPageId)pid, data);
		} catch (IOException e) {
			throw new NoSuchElementException();
		}
		return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    		int pageSize = BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        byte[] data = new byte[pageSize];
        data = page.getPageData();

        int position = page.getId().getPageNumber() * pageSize;
        randomAccessFile.seek(position);
        randomAccessFile.write(data, 0, pageSize);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    		return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<>();
        Page openPage = getOpenPage(tid);

        if (openPage == null) {
            HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
            newPage.insertTuple(t);
            writePage(newPage);
            modifiedPages.add(newPage);
        } else {
            HeapPage openHeapPage = (HeapPage) openPage;
            openHeapPage.insertTuple(t);
            modifiedPages.add(openHeapPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    		ArrayList<Page> modifiedPages = new ArrayList<Page>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);

        heapPage.deleteTuple(t);
//        heapPage.markDirty(true, tid);
        modifiedPages.add(heapPage);
        return modifiedPages;
    }
//    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
//    TransactionAbortedException {
//// some code goes here
//RecordId rid=t.getRecordId();
//PageId pid=rid.getPageId();
//Page p=Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
//HeapPage hp=(HeapPage)p;
//hp.deleteTuple(t);
//hp.markDirty(true,  tid);
//return hp;
//// return Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
//// not necessary for proj1
//}

    public Page getOpenPage(TransactionId transactionId) throws DbException, TransactionAbortedException{
        for(int i = 0; i < this.numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
            if (heapPage.getNumEmptySlots() > 0) {
                return heapPage;
            }
        }
        return null;
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    		return new HeapFileIterator(this,tid);
    }

}

class HeapFileIterator implements DbFileIterator {

	TransactionId tid;
    HeapFile heapFile;
    int pgNo;
    Iterator<Tuple> iterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }

    private List<Tuple> readTupleListFromPage(int pgNo) throws TransactionAbortedException, DbException{
        
        PageId pageId = new HeapPageId(heapFile.getId(), pgNo);
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        
        List<Tuple> tupleList = new ArrayList<Tuple>();
        
        // get all tuples from the first page in the file
        HeapPage heapPage = (HeapPage)page;
        Iterator<Tuple> itr = heapPage.iterator();
        while(itr.hasNext()){
            tupleList.add(itr.next());
        }
        return tupleList;
    }
    
    public boolean hasNext() throws DbException, TransactionAbortedException {
    		if( iterator == null){
            return false;
        }
        if(iterator.hasNext()){
            return true;
        }
        
        if (pgNo < heapFile.numPages()-1 && readTupleListFromPage(pgNo + 1).size() != 0){
            return true;
        }
        return false;
    }
    
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if(iterator == null){
            throw new NoSuchElementException();
        }
        
        if(iterator.hasNext()){
            return iterator.next();
        }
        
        if(!iterator.hasNext() && pgNo < heapFile.numPages()-1) {
            pgNo++;
            iterator = readTupleListFromPage(pgNo).iterator();
            if (iterator.hasNext()) return iterator.next();   
        }
        throw new NoSuchElementException();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void open() throws DbException, TransactionAbortedException {
    		this.pgNo = 0;
    		this.iterator = readTupleListFromPage(this.pgNo).iterator();
    }
    
    public void close() {
        this.iterator = null;
    }
}

