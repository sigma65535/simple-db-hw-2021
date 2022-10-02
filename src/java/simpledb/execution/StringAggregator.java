package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import javax.swing.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Integer,Integer> count;
    private HashMap<Integer,Tuple> tupData;
    private HashMap<Integer,Integer> valueMap;

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
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;

        this.valueMap = new HashMap<>();
        this.tupData = new HashMap<>();
        this.count = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int gbVal = ((IntField)tup.getField(this.gbfield)).getValue();
        String agVal = ((StringField)tup.getField(this.afield)).getValue();
        if (this.gbfield == NO_GROUPING) {
            this.gbfieldtype = null;
        }
        int newAggVal = this.count.getOrDefault(gbVal,0);
        if (this.what == Op.COUNT) {
            newAggVal = newAggVal + 1;
        }

        this.valueMap.put(gbVal,newAggVal);
        TupleDesc td = tup.getTupleDesc();
        Tuple newTup = new Tuple(td);
        newTup.setField(this.gbfield, new IntField(gbVal));
        newTup.setField(this.afield, new IntField(newAggVal));


        this.tupData.put(gbVal,newTup);
        this.count.put(gbVal,newAggVal);
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
        return new StringAggregatorOpIterator(tupData) ;
    }

}


class StringAggregatorOpIterator implements OpIterator {

    private HashMap<Integer,Tuple> tupData;
    private Iterator<Tuple> iterator;
    public StringAggregatorOpIterator(HashMap<Integer,Tuple> data) {
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

