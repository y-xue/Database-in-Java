//package simpledb;
//
//import java.io.*;
//import java.util.HashMap;
//import java.util.List;
//import java.util.concurrent.ConcurrentHashMap;
//
//import com.sun.javafx.collections.MappingChange.Map;
//
//import javafx.scene.Camera;
//import java.util.ArrayList;
//import java.util.Random;
//
///**
// * BufferPool manages the reading and writing of pages into memory from
// * disk. Access methods call into it to retrieve pages, and it fetches
// * pages from the appropriate location.
// * <p>
// * The BufferPool is also responsible for locking;  when a transaction fetches
// * a page, BufferPool checks that the transaction has the appropriate
// * locks to read/write the page.
// * 
// * @Threadsafe, all fields are final
// */
//public class BufferPool {
//    /** Bytes per page, including header. */
//    private static final int DEFAULT_PAGE_SIZE = 4096;
//
//    private static int pageSize = DEFAULT_PAGE_SIZE;
//    
//    /** Default number of pages passed to the constructor. This is used by
//    other classes. BufferPool should use the numPages argument to the
//    constructor instead. */
//    public static final int DEFAULT_PAGES = 50;
//
//    private int numPages;
//    
//    private HashMap<PageId,Page> pageMap;
//    
//    
//    /**
//     * Creates a BufferPool that caches up to numPages pages.
//     *
//     * @param numPages maximum number of pages in this buffer pool.
//     */
//    public BufferPool(int numPages) {
//        // some code goes here
//    		this.numPages = numPages;
//    		this.pageMap = new HashMap<>(numPages);
//    }
//    
//    public static int getPageSize() {
//      return pageSize;
//    }
//    
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void setPageSize(int pageSize) {
//    	BufferPool.pageSize = pageSize;
//    }
//    
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void resetPageSize() {
//    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
//    }
//    
//    /**
//     * Retrieve the specified page with the associated permissions.
//     * Will acquire a lock and may block if that lock is held by another
//     * transaction.
//     * <p>
//     * The retrieved page should be looked up in the buffer pool.  If it
//     * is present, it should be returned.  If it is not present, it should
//     * be added to the buffer pool and returned.  If there is insufficient
//     * space in the buffer pool, a page should be evicted and the new page
//     * should be added in its place.
//     *
//     * @param tid the ID of the transaction requesting the page
//     * @param pid the ID of the requested page
//     * @param perm the requested permissions on the page
//     */
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//        throws TransactionAbortedException, DbException {
//        // some code goes here
//    		if (!pageMap.containsKey(pid)) {
//    			// TODO: check if the map is full.
//    			if (this.pageMap.size() >= numPages)
//    				evictPage();
//    			int tableid = pid.getTableId();
//        		Catalog catalog = Database.getCatalog();
//        		DbFile dbFile = catalog.getDatabaseFile(tableid);
//        		Page page = dbFile.readPage(pid);
//    			pageMap.put(pid, page);
//    			return page;
//    		}
//    		else {
//    			return pageMap.get(pid);
//    		}
//    }
//
//    /**
//     * Releases the lock on a page.
//     * Calling this is very risky, and may result in wrong behavior. Think hard
//     * about who needs to call this and why, and why they can run the risk of
//     * calling it.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param pid the ID of the page to unlock
//     */
//    public  void releasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /**
//     * Release all locks associated with a given transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     */
//    public void transactionComplete(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /** Return true if the specified transaction has a lock on the specified page */
//    public boolean holdsLock(TransactionId tid, PageId p) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        return false;
//    }
//
//    /**
//     * Commit or abort a given transaction; release all locks associated to
//     * the transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param commit a flag indicating whether we should commit or abort
//     */
//    public void transactionComplete(TransactionId tid, boolean commit)
//        throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /**
//     * Add a tuple to the specified table on behalf of transaction tid.  Will
//     * acquire a write lock on the page the tuple is added to and any other 
//     * pages that are updated (Lock acquisition is not needed for lab2). 
//     * May block if the lock(s) cannot be acquired.
//     * 
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have 
//     * been dirtied to the cache (replacing any existing versions of those pages) so 
//     * that future requests see up-to-date pages. 
//     *
//     * @param tid the transaction adding the tuple
//     * @param tableId the table to add the tuple to
//     * @param t the tuple to add
//     */
//    public void insertTuple(TransactionId tid, int tableId, Tuple t)
//        throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//    		ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
//        markDirty(tid, dirtyPages);
//    }
//
//    /**
//     * Remove the specified tuple from the buffer pool.
//     * Will acquire a write lock on the page the tuple is removed from and any
//     * other pages that are updated. May block if the lock(s) cannot be acquired.
//     *
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have 
//     * been dirtied to the cache (replacing any existing versions of those pages) so 
//     * that future requests see up-to-date pages. 
//     *
//     * @param tid the transaction deleting the tuple.
//     * @param t the tuple to delete
//     */
//    public  void deleteTuple(TransactionId tid, Tuple t)
//        throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//    		int tableId = t.getRecordId().getPageId().getTableId();
//        ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
//        markDirty(tid, dirtyPages);
//    		
//    }
//
//    /*
//     * Marks page array to be dirty and places them into the buffer pool.
//     */
//    private void markDirty(TransactionId transcationId, ArrayList<Page> pages) {
//        for(Page page : pages) {
//            page.markDirty(true, transcationId);
//            pageMap.put(page.getId(), page);
//        }
//    }
//    
//    /**
//     * Flush all dirty pages to disk.
//     * NB: Be careful using this routine -- it writes dirty data to disk so will
//     *     break simpledb if running in NO STEAL mode.
//     */
//    public synchronized void flushAllPages() throws IOException {
//        // some code goes here
//        // not necessary for lab1
//    		for (PageId pageId : pageMap.keySet()) {
//            flushPage(pageId);
//        }
//
//    }
//
//    /** Remove the specific page id from the buffer pool.
//        Needed by the recovery manager to ensure that the
//        buffer pool doesn't keep a rolled back page in its
//        cache.
//        
//        Also used by B+ tree files to ensure that deleted pages
//        are removed from the cache so they can be reused safely
//    */
//    public synchronized void discardPage(PageId pid) {
//        // some code goes here
//        // not necessary for lab1
//    		pageMap.remove(pid);
//    }
//
//    /**
//     * Flushes a certain page to disk
//     * @param pid an ID indicating the page to flush
//     */
//    private synchronized  void flushPage(PageId pid) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//    		int tableId = pid.getTableId();
//        Page page = pageMap.get(pid);
//        Database.getCatalog().getDatabaseFile(tableId).writePage(page);
//        page.markDirty(false, page.isDirty());
//    }
//
//    /** Write all pages of the specified transaction to disk.
//     */
//    public synchronized  void flushPages(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /**
//     * Discards a page from the buffer pool.
//     * Flushes the page to disk to ensure dirty pages are updated on disk.
//     */
//    private synchronized  void evictPage() throws DbException {
//        // some code goes here
//        // not necessary for lab1
//    		Object[] pageIds = pageMap.keySet().toArray();
//        PageId toEvict = (PageId) pageIds[(int) Math.floor(Math.random() * pageIds.length)];
//
//        try {
//            flushPage(toEvict);
//            pageMap.remove(toEvict);
//        } catch (IOException e) {
//            throw new DbException("Could not flush page");
//        }
//    }
//
//}

// version 1

//package simpledb;
//
//import java.io.*;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * BufferPool manages the reading and writing of pages into memory from
// * disk. Access methods call into it to retrieve pages, and it fetches
// * pages from the appropriate location.
// * <p>
// * The BufferPool is also responsible for locking;  when a transaction fetches
// * a page, BufferPool checks that the transaction has the appropriate
// * locks to read/write the page.
// */
//public class BufferPool {
//    /** 
//     * 
//     */
//    private volatile LockManager lockManager;
//    /** Bytes per page, including header. */
//    
//    private static final int DEFAULT_PAGE_SIZE = 4096;
//    private static int pageSize = DEFAULT_PAGE_SIZE;
//
//    /** param for buffer capacity numPages*/
//    private volatile int numPages;
//    /** structure to store buffer pages*/
//    private volatile Map<PageId, Page> bufferMap;//pageId, page
//
//	private volatile Map<PageId, Integer> recentlyUsed;
//	
//	private volatile Map<TransactionId, Long> allTransactions;
//
//
//    /** Default number of pages passed to the constructor. This is used by
//    other classes. BufferPool should use the numPages argument to the
//    constructor instead. */
//    public static final int DEFAULT_PAGES = 50;
//
//    /**
//     * Creates a BufferPool that caches up to numPages pages.
//     *
//     * @param numPages maximum number of pages in this buffer pool.
//     */
//    public BufferPool(int numPages) {
//        // some code goes here
//        this.numPages=numPages;
//        bufferMap=new ConcurrentHashMap<PageId, Page>();
//	    recentlyUsed=new ConcurrentHashMap<PageId, Integer>();
//	allTransactions = new ConcurrentHashMap<TransactionId, Long>();
//        lockManager = new LockManager();
//
//    }
//    
//    public static int getPageSize() {
//        return pageSize;
//    }
//
//    /**
//     * Retrieve the specified page with the associated permissions.
//     * Will acquire a lock and may block if that lock is held by another
//     * transaction.
//     * <p>
//     * The retrieved page should be looked up in the buffer pool.  If it
//     * is present, it should be returned.  If it is not present, it should
//     * be added to the buffer pool and returned.  If there is insufficient
//     * space in the buffer pool, an page should be evicted and the new page
//     * should be added in its place.
//     *
//     * @param tid the ID of the transaction requesting the page
//     * @param pid the ID of the requested page
//     * @param perm the requested permissions on the page
//     */
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//        throws TransactionAbortedException, DbException{
//        //check whether to grant lock for this transaction
//
//if (!allTransactions.containsKey(tid)) {//if this is new transaction
//        long t0 = System.currentTimeMillis();
//	allTransactions.put(tid, t0);
//	
//        boolean notGranted = lockManager.grantLock(pid, tid, perm);
//        //put on queue if the lock is not granted
//        while( notGranted){
//            //long t1 = System.currentTimeMillis();
//            //Rex: you can tweak these numbers, originally kept requesting not working/slow
//            if ((System.currentTimeMillis() - allTransactions.get(tid)) > 250) {
//                //lockManager.releaseAllTidLocks(tid);
//                throw new TransactionAbortedException();
//            }
//            try {
//                Thread.sleep(200);
//                notGranted = lockManager.grantLock(pid, tid, perm);
//            } catch (InterruptedException e){
//                e.printStackTrace();
//            }
//        }//ends while
//} else {//is an already running transsaction
//
//        boolean notGranted = lockManager.grantLock(pid, tid, perm);
//        //put on queue if the lock is not granted
//        while( notGranted){
//            //long t1 = System.currentTimeMillis();
//            //Rex: you can tweak these numbers, originally kept requesting not working/slow
//            if ((System.currentTimeMillis() - allTransactions.get(tid)) > 500) {
//                //lockManager.releaseAllTidLocks(tid);
//                throw new TransactionAbortedException();
//            }
//            try {
//                Thread.sleep(10);
//                notGranted = lockManager.grantLock(pid, tid, perm);
//            } catch (InterruptedException e){
//                e.printStackTrace();
//            }
//        }//ends while
//}
//
//
//        // some code goes here
//        //check if the page is in the bufferMap
//
//        if (bufferMap.containsKey(pid)){
//            //update the recentlyUsed hashmap
//            updateRecentlyUsed();
//		  recentlyUsed.put(pid, 0);
//          //this page was just accessed
//        return bufferMap.get(pid);
//
//        } else {
//            List<Catalog.Table>tableList=Database.getCatalog().getTables();
//            for (Catalog.Table t: tableList){
//                if (t.get_file().getId()==pid.getTableId()){
//                    DbFile file=t.get_file();
//                    Page pageRead=file.readPage(pid);
//                    if (numPages<=bufferMap.size()){
//			//will have to deal with eviction here
//			evictPage();
//                    }
//                    bufferMap.put(pid,pageRead);
//			updateRecentlyUsed();
//			recentlyUsed.put(pid, 0);//this page was just accessed
//                    return pageRead;  
//                }           
//            }
//
//        }      
//            throw new DbException("page requested not in bufferpool or disk");
//    }
//
//    /**
//     * Releases the lock on a page.
//     * Calling this is very risky, and may result in wrong behavior. Think hard
//     * about who needs to call this and why, and why they can run the risk of
//     * calling it.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param pid the ID of the page to unlock
//     */
//    public void releasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for proj1
//        lockManager.releaseLock(pid, tid);
//    }
//
//    /**
//     * Release all locks associated with a given transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     */
//    public void transactionComplete(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for proj1
//        transactionComplete(tid, true);
//    }
//
//    /** Return true if the specified transaction has a lock on the specified page */
//    public boolean holdsLock(TransactionId tid, PageId p) {
//        // some code goes here
//        // not necessary for proj1
//        return lockManager.holdsLock(tid, p);
//    }
//
//    /**
//     * Commit or abort a given transaction; release all locks associated to
//     * the transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param commit a flag indicating whether we should commit or abort
//     */
//    public void transactionComplete(TransactionId tid, boolean commit)
//        throws IOException {
//        // some code goes here
//        // not necessary for proj1
//	//commit: flush transaction pages
//	//abort: revert changes
//	//release BufferPool states related to transaction, release locks
//
//	allTransactions.remove(tid);
//	if (commit == true) {
//        for (Page page : bufferMap.values()) {
//            if (page.isDirty()!=null && page.isDirty().equals(tid)) {
//                flushPages(tid);
//                page.setBeforeImage();
//            }
//
//            if (page.isDirty() == null){
//                page.setBeforeImage();
//            }
//        }
//	} else {
//		for (Page page : bufferMap.values()) {
//			if (page.isDirty()!=null && page.isDirty().equals(tid)) {
//				bufferMap.put(page.getId(), page.getBeforeImage());
//			}
//		}
//	}
//	//release all locks related to transaction
//	lockManager.releaseAllTidLocks(tid);
//
//
//    }
//
//    /**
//     * Add a tuple to the specified table behalf of transaction tid.  Will
//     * acquire a write lock on the page the tuple is added to(Lock 
//     * acquisition is not needed for lab2). May block if the lock cannot 
//     * be acquired.
//     * 
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and updates cached versions of any pages that have 
//     * been dirtied so that future requests see up-to-date pages. 
//     *
//     * @param tid the transaction adding the tuple
//     * @param tableId the table to add the tuple to
//     * @param t the tuple to add
//     */
//    public void insertTuple(TransactionId tid, int tableId, Tuple t)
//        throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for proj1
//        try{
//        	ArrayList<Page> affectedPages;
//        	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
//        	HeapFile heapFile = (HeapFile)dbFile;
//        	affectedPages = heapFile.insertTuple(tid, t);
//        	//iterate through affectedPages and markDirty
//		//also update cached pages
//                
//            int size = affectedPages.size();
//        	for (Page page : affectedPages) {
//    		page.markDirty(true,tid);
//			bufferMap.put(page.getId(), page);
//        	}
//        }
//        catch (DbException e){
//                e.printStackTrace();
//            }
//        catch (TransactionAbortedException e){
//                e.printStackTrace();
//            }
//        catch (IOException e){
//                e.printStackTrace();
//            }
//
//    }
//
//    /**
//     * Remove the specified tuple from the buffer pool.
//     * Will acquire a write lock on the page the tuple is removed from. May block if
//     * the lock cannot be acquired.
//     *
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit.  Does not need to update cached versions of any pages that have 
//     * been dirtied, as it is not possible that a new page was created during the deletion
//     * (note difference from addTuple).
//     *
//     * @param tid the transaction adding the tuple.
//     * @param t the tuple to add
//     */
//    public void deleteTuple(TransactionId tid, Tuple t)
//        throws DbException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for proj1
//        try
//        {
//    	int tableId = t.getRecordId().getPageId().getTableId(); 
//    	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
//    	HeapFile heapFile = (HeapFile)dbFile;
//    	List<Page> affectedPages = heapFile.deleteTuple(tid, t);
////    	Page affectedPage = affectedPages.get(0);
//    	//iterate through affectedPages and markDirty
////        affectedPage.markDirty(true,tid);
//    	for (Page page : affectedPages) {
//    		page.markDirty(true, tid);
//    	}
//        }
//        catch (DbException e){
//                e.printStackTrace();
//            }
//        catch (TransactionAbortedException e){
//                e.printStackTrace();
//            }
//    }
//
//    /**
//     * Flush all dirty pages to disk.
//     * NB: Be careful using this routine -- it writes dirty data to disk so will
//     *     break simpledb if running in NO STEAL mode.
//     */
//	//call flushPage on all pages in the bp
//    public synchronized void flushAllPages() throws IOException {
//        // some code goes here
//        // not necessary for proj1
//    	for (PageId key : bufferMap.keySet()) {
//    		flushPage(key);
//    	}
//
//    }
//
//    /** Remove the specific page id from the buffer pool.
//        Needed by the recovery manager to ensure that the
//        buffer pool doesn't keep a rolled back page in its
//        cache.
//    */
//    public synchronized void discardPage(PageId pid) {
//        // some code goes here
//    // not necessary for proj1
//        bufferMap.remove(pid);
//    }
//
//    /**
//     * Flushes a certain page to disk
//     * @param pid an ID indicating the page to flush
//     */
//    //write page to dsk and mark as not dirty
//    private synchronized  void flushPage(PageId pid) throws IOException {
//        // some code goes here
//        // not necessary for proj1
//    	Page page = bufferMap.get(pid);
//    	int tableId = ((HeapPageId)pid).getTableId();
//    	HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
//    	hf.writePage(page);
//    	page.markDirty(false, null);
//	
//    }
//
//    /** Write all pages of the specified transaction to disk.
//	* NEED FOR PROJ2?????
//     */
//    public synchronized  void flushPages(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for proj1
//        for (Page page : bufferMap.values()) {
//        	if (page.isDirty() !=null && page.isDirty()==tid) {
//        		flushPage(page.getId());
//        	}
//        }
//    }
//
//    /**
//     * Discards a page from the buffer pool.
//     * Flushes the page to disk to ensure dirty pages are updated on disk.
//     */
//    private synchronized void evictPage() throws DbException {
//        // some code goes here
//        // not necessary for proj1
//    	Page evictedPage;
//    	int counter = -1;
//    	PageId evictedPageId = null;
//        boolean isPageDirty = true;
//        int dirtyPageCount = 0;
//      
//        for (PageId key : bufferMap.keySet()) {
//            isPageDirty = ((HeapPage)bufferMap.get(key)).isDirty() != null;
//            if (isPageDirty){
//                dirtyPageCount++;
//            }
//        }
//        //if all pages are dirty
//        if (dirtyPageCount == numPages){
//            throw new DbException("all pages in BufferPool are dirty.");
//        }
//        isPageDirty = true;
//        //Check to make sure that the page evicted is not a dirty page
//    	for (PageId key : bufferMap.keySet()) {
//    		int value = recentlyUsed.get(key);
//    		if (value > counter) {	
//    			counter = value;
//    			evictedPageId = key;
//                evictedPage = bufferMap.get(evictedPageId);
//                isPageDirty = ((HeapPage)evictedPage).isDirty() != null;
//                if (!isPageDirty){
//                    try{
//                        flushPage(evictedPageId);
//                        bufferMap.remove(evictedPageId);
//                        recentlyUsed.remove(evictedPageId);
//                        break;
//                        
//                        }catch (IOException e){
//                            e.printStackTrace();
//                        }
//                }
//
//            }
//        }
//       
//    }
//
//    public void updateRecentlyUsed() {
//    	if (!recentlyUsed.isEmpty()){
//    		for (PageId key : recentlyUsed.keySet()) {
//    			int value = recentlyUsed.get(key);
//    			value++;
//    			recentlyUsed.put(key, value);	
//    		}
//    	}
//    }
//
//    /**
//     * a LockManager class to keep track of locks. 
//     */
//    private class LockManager {
//
//        private Map<PageId, Set<TransactionId>> pageReadLocks;
//        private Map<PageId, TransactionId> pageWriteLocks;
//        private Map<TransactionId, Set<PageId>> sharedPages;
//        private Map<TransactionId, Set<PageId>> exclusivePages;
//
//
//        /**
//         *The constructor of the lockmanager
//         */
//        public LockManager(){
//            pageReadLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
//            pageWriteLocks = new ConcurrentHashMap<PageId, TransactionId>();
//            sharedPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
//            exclusivePages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
//        }
//
//        /**
//         * check to see if a transaction has a lock on a page
//         * @param  tid specified TransactionID
//         * @param  pid specified PageId
//         * @return     true if tid has a lock on pid
//         */
//        public boolean holdsLock(TransactionId tid, PageId pid){
//            Set<TransactionId> tidSet;
//            TransactionId writetid; 
//            tidSet = pageReadLocks.get(pid);
//            writetid = pageWriteLocks.get(pid);
//            return (tidSet.contains(tid) || writetid.equals(tid));
//        }
//
//        /**
//         * release a transaction's lock on a page specified by pid
//         * @param pid pageId of this page
//         * @param tid TransactionId of this transaction
//         */
//        public synchronized void releaseLock(PageId pid, TransactionId tid){
//            Set<PageId> pidSet = sharedPages.get(tid);
//            Set<TransactionId> tidSet = pageReadLocks.get(pid);
//            Set<PageId> exclusivePidSet = exclusivePages.get(tid);
//            if (tidSet!= null){
//                tidSet.remove(tid);
//                pageReadLocks.put(pid, tidSet);
//            }
//            pageWriteLocks.remove(pid);
//            if (pidSet != null){
//                pidSet.remove(pid);
//                sharedPages.put(tid, pidSet);
//            }
//            if (exclusivePidSet != null) {
//                exclusivePidSet.remove(pid);
//                exclusivePages.put(tid, exclusivePidSet);
//            }
//        }
//		
//		// ask LockManager to release all the locks related to a transaction, for transactionComplete()
//        public synchronized void releaseAllTidLocks(TransactionId tid){
//        	for (PageId pageId : pageWriteLocks.keySet()) {
//        		if (pageWriteLocks.get(pageId) != null && pageWriteLocks.get(pageId)==tid) {
//        			 pageWriteLocks.remove(pageId);
//        		}
//        	}
//            exclusivePages.remove(tid);
//    	
//        	for (PageId pageId : pageReadLocks.keySet()) {
//        		Set<TransactionId> tidSet = pageReadLocks.get(pageId);
//        		if (tidSet != null) {
//        			tidSet.remove(tid);
//                    pageReadLocks.put(pageId, tidSet);
//
//        		}
//        	}
//            sharedPages.remove(tid);
//
//        	
//	}
//
//        public synchronized boolean grantLock(PageId pid, TransactionId tid, Permissions perm){
//
//            if (perm.equals(Permissions.READ_ONLY)){
//                Set<TransactionId> tidSet = pageReadLocks.get(pid);
//                TransactionId writetid = pageWriteLocks.get(pid);
//                if (writetid == null || writetid.equals(tid)) {
//
//                    if (tidSet == null){
//                        tidSet = new HashSet<TransactionId>();
//                    }
//
//                    tidSet.add(tid);
//                    pageReadLocks.put(pid, tidSet);
//
//
//                    Set<PageId> pageIdSet = sharedPages.get(tid);
//                    if (pageIdSet == null) {
//                        pageIdSet = new HashSet<PageId>();
//                    }
//                    pageIdSet.add(pid);
//                    sharedPages.put(tid, pageIdSet);
//                    return false;
//
//                } else {
//                    return true;
//                }
//
//                //If this is a Read and Write
//            } else {
//                Set<TransactionId> tidSet = pageReadLocks.get(pid);
//                TransactionId writetid = pageWriteLocks.get(pid);
//
//                if (tidSet != null && tidSet.size() > 1){
//                    return true;
//                }
//                if (tidSet != null && tidSet.size() == 1 && !tidSet.contains(tid)){
//                    return true;
//                }
//                if (writetid != null && !writetid.equals(tid)){
//                    return true;
//                } else {
//                    pageWriteLocks.put(pid, tid);
//                    Set<PageId> pidSet = exclusivePages.get(tid);
//                    if (pidSet == null){
//                        pidSet = new HashSet<PageId>();
//                    }
//                    pidSet.add(pid);
//                    exclusivePages.put(tid, pidSet);
//                    return false;
//
//                }
//            }
//        }
//
//    }
//}


// version 2
package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;
	
	private static int pageSize = DEFAULT_PAGE_SIZE;
	private final Random random = new Random();
	
	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	private int numPages;
	private ConcurrentHashMap<PageId, Page> pageMap;
	
	private final LockManager lockManager;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		this.numPages = numPages;
		pageMap = new ConcurrentHashMap<PageId, Page>();
		this.lockManager = new LockManager();

	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		 try {
			 lockManager.acquireLock(tid, pid, perm);
		} catch (DeadlockException de) {
			lockManager.releaseAll(tid, false);
			throw new TransactionAbortedException();
		}
		
		if (!this.pageMap.containsKey(pid)) {
			
			if (this.pageMap.size() >= this.numPages) {
				this.evictPage();
			}
			Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
			this.pageMap.put(pid, newPage);
			return newPage;
		}
		else {
			return this.pageMap.get(pid);
		}
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		lockManager.releaseLock(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return lockManager.holdsLock(tid,p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2

		if (commit) {
		  for (Page page : pageMap.values()) {
		      if (page.isDirty()!=null && page.isDirty().equals(tid)) {
		          flushPages(tid);
		          page.setBeforeImage();
		      }
		
		      if (page.isDirty() == null){
		          page.setBeforeImage();
		      }
		  }
		} else {
			for (Page page : pageMap.values()) {
				if (page.isDirty()!=null && page.isDirty().equals(tid)) {
					pageMap.put(page.getId(), page.getBeforeImage());
				}
			}
		}
		//release all locks for this tid
		lockManager.releaseAll(tid, commit);
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2). May
	 * block if the lock(s) cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
	    // not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(tableId);

		ArrayList<Page> dirtypages = file.insertTuple(tid, t);

		synchronized (this) {
			for (Page p : dirtypages) {
				p.markDirty(true, tid);
				
				// if page in pool already, done.
				if (pageMap.get(p.getId()) != null) {
					// replace old page with new one in case addTuple returns a
					// new copy of the page
					pageMap.put(p.getId(), p);
				} else {
					// put page in pool
					if (pageMap.size() >= numPages)
						evictPage();
					pageMap.put(p.getId(), p);
				}
			}
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from and any other pages that are
	 * updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction deleting the tuple.
	 * @param t
	 *            the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) 
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
	    // not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirtypages = file.deleteTuple(tid, t);

		synchronized (this) {
			for (Page p : dirtypages) {
				p.markDirty(true, tid);

				// if page in pool already, done.
				if (pageMap.get(p.getId()) != null) {
					// replace old page with new one in case deleteTuple returns
					// a new copy of the page
					pageMap.put(p.getId(), p);
				} else {

					// put page in pool
					if (pageMap.size() >= numPages)
						evictPage();
					pageMap.put(p.getId(), p);
				}
			}
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
	    // not necessary for lab1
		Iterator<PageId> i = pageMap.keySet().iterator();
		while (i.hasNext()) {
			flushPage(i.next());
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from
	 * the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
	    // not necessary for lab1
		Page p = pageMap.get(pid);
		if (p != null) {
			pageMap.remove(pid);
		}
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
	    // not necessary for lab1
		
		Page p = pageMap.get(pid);
		if (p == null) {
			return; // not in buffer pool -- doesn't need to be flushed
		}
		
		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
		file.writePage(p);
		p.markDirty(false, null);
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
	    // not necessary for lab1|lab2
		
		Set<PageId> pageId = lockManager.getLockedPages(tid);
		if (pageId == null) return;
		
		for (PageId p : pageId) {
		    flushPage(p);
		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
	    // not necessary for lab1
		
		Object pids[] = pageMap.keySet().toArray();
		PageId pid = (PageId) pids[random.nextInt(pids.length)];
		try {
			Page p = pageMap.get(pid);
			if (p.isDirty() != null) {
				// if the current page is dirty, remove it first
				boolean findNewPage = false;
				for (PageId pg : pageMap.keySet()) {
					if (pageMap.get(pg).isDirty() == null) {
						pid = pg;
						findNewPage = true;
						break;
					}
				}
				if (!findNewPage) {
					throw new DbException("Dirty pages in all slots.");
				}
			}
			flushPage(pid);
		} catch (IOException e) {}
		pageMap.remove(pid);
	}

    class LockManager {

        final HashMap<TransactionId,Set<PageId>> tidPages;
        final HashMap<PageId,Set<TransactionId>> pagetids;
        final HashMap<PageId,Permissions> pagePermission;
        final HashMap<TransactionId, Vector<TransactionId>> tidtids;

        LockManager() {
            tidPages = new HashMap<>();
            pagetids = new HashMap<>();
            pagePermission = new HashMap<>();
            tidtids = new HashMap<>();
        }

        /**
         * Acquire a lock for (tid,pid,perm).
         * Throw DeadlockException if can't acquire one.
         */
        @SuppressWarnings("unchecked")
        public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws DeadlockException {

            while(!lock(tid, pid, perm)) {

                synchronized(this) {
                    Vector<TransactionId> v = tidtids.get(tid);
                    if (v == null) {
                        v = new Vector<TransactionId>();
                        tidtids.put(tid,v);
                    } 
                    Set<TransactionId> lockHolder = pagetids.get(pid);
                    if (lockHolder != null) {
                    		lockHolder = (Set<TransactionId>)((HashSet)lockHolder).clone();
                    		lockHolder.remove(tid);
	                    v.addAll(lockHolder);
	                    tidtids.put(tid,v);
	                    if (isWaitingForDeadlock(tid)) {
	                        tidtids.remove(tid);
	                        throw new DeadlockException();
	                    }
                    }
                }
            }
            
            synchronized(this) {
            		tidtids.remove(tid); 
            }

            return true;
        }
        
        /**
         * Check if (tid,pid,perm) is already locked.
         * if not, it can acquire the lock.
         */
        private boolean isLocked(TransactionId tid, PageId pid, Permissions perm) {
            Set<PageId> pidset = null;
            Set<TransactionId> tset = null;
            Permissions p = null;

            synchronized (this){
            		pidset = tidPages.get(tid);
	            tset = pagetids.get(pid);
	            p = pagePermission.get(pid);
            }
            
            // no lock is held on this page
            if (p == null) return false;

            if (perm == Permissions.READ_ONLY) {
            		// READ_ONLY
                
            		// if transaction is holding lock on pid
            		// grant lock
                if (pidset != null && pidset.contains(pid))
                    return false;

                // if another transaction is holding a READ lock on pid
                // grant lock
                if (p == Permissions.READ_ONLY)
                    return false;

                // if another transaction is holding a READ_WRITE lock on pid
                // can't grant lock
                if (p == Permissions.READ_WRITE)
                    return true;
                
            } else {
            		// READ_WRITE
                
            		// if only this transaction is holding a READ lock on pid
            		// grant lock
                if (p == Permissions.READ_ONLY && tset.contains(tid) && tset.size() == 1)
                    return false;

                // if transaction is holding a WRITE lock on pid
                // grant lock
                if (p == Permissions.READ_WRITE && tset.contains(tid))
                    return false;

                // if another transaction is holding a lock on pid
                // wait and go back to start
                if (tset.size() != 0)
                    return true;
            }
            System.out.println("shouldn't see me");
            return true;
        }
        
        /**
         * Lock (tid, pid, perm)
         */
        private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {

            if(isLocked(tid, pid, perm)) {
                return false;
            }

            Set<PageId> pidset = tidPages.get(tid);

            if (pidset == null) {
            		pidset = new HashSet<PageId>();
                tidPages.put(tid, pidset);
            }
            if (!pidset.contains(pid)) {
            		pidset.add(pid);
            }

            Set<TransactionId> tset = pagetids.get(pid);
            if(tset == null) {
                tset = new HashSet<TransactionId>();
                pagetids.put(pid, tset);
            }
            if (!tset.contains(tid)) {
            		tset.add(tid);
            }
            
            Permissions old = pagePermission.get(pid);
            if (old == null || (old == Permissions.READ_ONLY && perm == Permissions.READ_WRITE)) {
                pagePermission.put(pid, perm);
            }
            return true;
        }
        
        /**
         * Check if transaction is waiting for a deadlock
         */
        @SuppressWarnings("unchecked")
        public synchronized boolean isWaitingForDeadlock(TransactionId tid) {
            Vector graph = new Vector<TransactionId>();
            graph.addElement((TransactionId)tid);
            return checkWaitsForGraph(tid,graph);
        }
        
        /**
         * Helper function for isWaitingForDeadlock.
         * Check the WaitsFor graph.
         */
        @SuppressWarnings("unchecked")
        public synchronized boolean checkWaitsForGraph(TransactionId tid, Vector<TransactionId> graph) {

            Vector<TransactionId> tids = tidtids.get(tid);
            if (tids == null) return false;
            for (TransactionId ti : tids) {
                Vector<TransactionId> g = (Vector<TransactionId>)graph.clone();
                g.addElement(ti);
                if (graph.contains(ti) || checkWaitsForGraph(ti,g)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * release a transaction's lock on a page specified by pid
         */
        public  synchronized void releaseLock(TransactionId tid, PageId pid) {
           
            Set<PageId> pidSet = tidPages.get(tid);
            if (pidSet != null) {
            		pidSet.remove(pid);
                if(pidSet.isEmpty())
                    tidPages.remove(tid);
            }

            Set<TransactionId> tset = pagetids.get(pid);
            if (tset != null) {
                tset.remove(tid);
                if (tset.isEmpty()) {
                    pagetids.remove(pid);
                    pagePermission.remove(pid);
                }
            }
        }
        
        /**
         * release all transaction locks
         */
        public synchronized void releaseAll(TransactionId tid, boolean commit) {
            Set<PageId> s =  tidPages.get(tid);
           if (s == null) return;

            Set<PageId> pidset = new HashSet<PageId>(s);
            for (Iterator<PageId> i = pidset.iterator(); i.hasNext(); ) {

                PageId pid = i.next();
                if (commit) {
                    // if commit, flush page to disk
                    try {
                        Page p = pageMap.get(pid);
                        if (p != null) {
                            flushPage(pid);
                            p.setBeforeImage();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("failed during commit: " + e);
                    }
                } else if ((pagePermission.get(pid)).equals(Permissions.READ_WRITE)) {
                    Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pageMap.put(pid, p);
                }

                releaseLock(tid, pid);
            }
        }

        /**
         * Get the locked PageId for transaction
         */
        public synchronized Set<PageId> getLockedPages(TransactionId tid) {
            return tidPages.get(tid);
        }

        /**
         * check if a transaction has a lock on a page
         */
        public synchronized boolean holdsLock(TransactionId tid, PageId p) {
            Set<TransactionId> tset =  pagetids.get(p);
            if (tset == null) return false;
            return tset.contains(tid);
        }
    }
    
 }
