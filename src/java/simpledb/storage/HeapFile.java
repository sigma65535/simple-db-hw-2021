package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


      private File file;
    private TupleDesc td;

    private int offset;
    private HeapPageId [] pageIdList;
    private int pageNum;


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
        return file;
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
        return  file.getAbsoluteFile().hashCode();
    }



    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        try {
            RandomAccessFile curFile = new RandomAccessFile(this.file, "r");
            byte[] data = new byte[pageSize];
            curFile.seek((long) pageNo * pageSize);
            curFile.read(data);
            HeapPage curPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
            curFile.close();
            return curPage;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        this.pageNum = (int) Math.ceil(file.length()*1.0/BufferPool.getPageSize());
        return this.pageNum;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
}


 class HeapFileIterator implements DbFileIterator {

    private int curPageNo;

    private int pageNum;

    private Iterator<Tuple> iterator;

    private HeapFile heapFile;

    private TransactionId tid;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.curPageNo = 0;
        this.pageNum = heapFile.numPages();
    }

    public Iterator<Tuple> getNextPage(HeapPageId id) throws TransactionAbortedException, DbException {
        BufferPool pool = Database.getBufferPool();  // ???bufferpool?????????page?????????????????????????????????getPage???????????????
        HeapPage page = (HeapPage) pool.getPage(this.tid, id, Permissions.READ_ONLY);
        return page.iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.curPageNo = 0;
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), 0);  // table???HeapFile????????????
        this.iterator = this.getNextPage(pageId);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.iterator == null) {
            return false;
        }
        if (this.iterator.hasNext()) {
            return true;
        } else {
            this.curPageNo += 1;
            if (this.curPageNo >= pageNum) {
                return false;
            }
            this.iterator = this.getNextPage(new HeapPageId(this.heapFile.getId(), this.curPageNo));
            if (this.iterator == null) {
                return false;
            }
            return this.iterator.hasNext();
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.iterator == null || !this.iterator.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public void close() {
        this.iterator = null;
    }
}