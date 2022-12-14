package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;
    private OpIterator child1;
    private OpIterator[] children;
    private OpIterator child2;
    private TupleIterator tupleIterator;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.joinPredicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        return null;
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return null;
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {

        TupleDesc tup1 = this.child1.getTupleDesc();
        TupleDesc tup2 = this.child2.getTupleDesc();

        return TupleDesc.merge(tup1, tup2);
    }

    private ArrayList<Tuple> tupList() throws TransactionAbortedException, DbException {

        ArrayList<Tuple> list = new ArrayList<>();

        while (this.child1.hasNext()) {
            Tuple tup1 = this.child1.next();
            while (this.child2.hasNext()) {
                Tuple tup2 = this.child2.next();

                if (this.joinPredicate.filter(tup1, tup2)) {

                    TupleDesc tupleDesc1 = tup1.getTupleDesc();
                    TupleDesc tupleDesc2 = tup2.getTupleDesc();
                    Tuple tup = new Tuple(TupleDesc.merge(tupleDesc1, tupleDesc2));
                    for (int i = 0; i < tupleDesc1.numFields(); i++) {
                        tup.setField(i,tup1.getField(i));
                    }
                    for (int i = 0; i < tupleDesc2.numFields(); i++) {
                        tup.setField(i+tupleDesc1.numFields(),tup2.getField(i));
                    }
                    list.add(tup);
                }



            }

            this.child2.rewind();

        }
        this.child1.rewind();
        return list;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {

        super.open();

        this.child1.open();
        this.child2.open();
        List<Tuple> tuplist = this.tupList();
        this.tupleIterator = new TupleIterator(this.getTupleDesc(),tuplist);

        this.tupleIterator.open();

    }

    public void close() {
        this.tupleIterator.close();
        this.child1.close();
        this.child2.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.tupleIterator.rewind();
        this.child1.rewind();
        this.child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.tupleIterator.hasNext()){
            return this.tupleIterator.next();
        }
        return null;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
