package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Integer,Integer> count;
    private HashMap<Integer,ArrayList<Integer>> historyData;

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
    private HashMap<Integer,Tuple> tupData;

    private HashMap<Integer,Integer> valueMap;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.valueMap = new HashMap<>();
        this.tupData = new HashMap<>();
        this.count = new HashMap<>();
        this.historyData = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int gbVal = NO_GROUPING;
        int gbfield = this.gbfield;
        if (this.gbfield == NO_GROUPING) {
            this.gbfieldtype = null;
        }else {
            gbVal = ((IntField)tup.getField(gbfield)).getValue();
        }

        int cnt = this.count.getOrDefault(gbVal,0);
        int aggVal = ((IntField)tup.getField(this.afield)).getValue();
        int newAggVal= 0;

        ArrayList<Integer> list = this.historyData.getOrDefault(gbVal,new ArrayList<>());
        list.add(aggVal);
        this.historyData.put(gbVal,list);


        switch (this.what){
            case SUM : newAggVal = this.valueMap.getOrDefault(gbVal,0)+aggVal;break;
            case MIN : newAggVal = Math.min(this.valueMap.getOrDefault(gbVal,Integer.MAX_VALUE),aggVal) ;break;
            case MAX : newAggVal = Math.max(this.valueMap.getOrDefault(gbVal,Integer.MIN_VALUE),aggVal) ;break;
            case AVG :

                for (Integer item :list) {
                    newAggVal+= item;
                }
                newAggVal = newAggVal/list.size();
                break;
            case COUNT:newAggVal = list.size();break;
        }

        this.valueMap.put(gbVal,newAggVal);
        TupleDesc td = tup.getTupleDesc();
        Tuple newTup = new Tuple(td);
        if (gbfield == NO_GROUPING) {
            newTup.setField(0, new IntField(newAggVal));
        }else  {
            newTup.setField(gbfield, new IntField(gbVal));
            newTup.setField(this.afield, new IntField(newAggVal));
        }

        this.tupData.put(gbVal,newTup);
        this.count.put(gbVal,cnt+1);


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
        return new IntegerAggregatorOpIterator(tupData) ;
    }

}

class IntegerAggregatorOpIterator implements OpIterator {

    private HashMap<Integer,Tuple> tupData;
    private Iterator<Tuple> iterator;
    public IntegerAggregatorOpIterator(HashMap<Integer,Tuple> data) {
        this.tupData = data;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {

        iterator = this.tupData.values().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.iterator.hasNext()){
            return this.iterator.next();
        }

        throw  new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

    @Override
    public void close() {
        this.iterator = null;
    }
}
