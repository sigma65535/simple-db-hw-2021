package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.NO_GROUPING;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private OpIterator[] children;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if (this.gfield == -1) {
            return NO_GROUPING;
        }
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (this.gfield != -1) {
            return this.getTupleDesc().getFieldName(this.gfield);
        }
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    private OpIterator aggIterator;

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        IntegerAggregator agg = new IntegerAggregator(this.gfield,Type.INT_TYPE,this.afield,this.aop);
        if (this.child.getTupleDesc().getFieldType(this.afield) == Type.INT_TYPE) {
            while (this.child.hasNext()) {
                Tuple tup = this.child.next();
                agg.mergeTupleIntoGroup(tup);
            }

            this.aggIterator = agg.iterator();
            this.aggIterator.open();


        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggIterator.hasNext()) {
            Tuple retTup  = this.aggIterator.next();


            Type[] types = new Type[2];
            String[] names = new String[2];

            gfield = this.gfield;
            if (gfield == NO_GROUPING) {
//                Field f0 = retTup.getField(gfield);
//                types[0] = f0.getType();
//                names[0] = "group by "+gfield;
                types = new Type[1];
                names = new String[1];
                Field f1 = retTup.getField(0);
                types[0] = f1.getType();
                names[0] = this.aop+" "+this.afield;

                Tuple  tp= new Tuple(new TupleDesc(types,names));
//                tp.setField(gfield,f0);
                tp.setField(0,f1);
                return tp;
            }else {
                Field f0 = retTup.getField(gfield);
                types[0] = f0.getType();
                names[0] = "group by "+gfield;

                Field f1 = retTup.getField(this.afield);
                types[1] = f1.getType();
                names[1] = this.aop+" "+this.afield;

                Tuple  tp= new Tuple(new TupleDesc(types,names));
                tp.setField(gfield,f0);
                tp.setField(this.afield,f1);
                return tp;
            }

        }


        return null;
    }




    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void close() {
        // some code goes here
        this.child.close();
        this.aggIterator.close();
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
