package main.execution.executors;

import main.edu.uci.db.execution.ExecutorContext;
import main.edu.uci.db.execution.expressions.AbstractExpression;
import main.edu.uci.db.execution.expressions.AggregateValueExpression;
import main.edu.uci.db.execution.plans.AggregationPlanNode;
import main.edu.uci.db.execution.plans.AggregationType;
import main.type.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AggregationExecutor extends AbstractExecutor {

    class AggregateKey {
        List<Value> groupBys;

        public AggregateKey(List<Value> groupBys) {
            this.groupBys = new ArrayList<>(groupBys);
        }

        /**
         * Compares two aggregate keys for equality.
         *
         * @param other the other aggregate key to be compared with
         * @return true if both aggregate keys have equivalent group-by expressions, false otherwise
         */
        public boolean equals(AggregateKey other) {
            if (groupBys.size() != other.groupBys.size())
                return false;
            for (int i = 0; i < other.groupBys.size(); i++) {
                if (!groupBys.get(i).equals(other.groupBys.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    class AggregateValue {
        List<Value> aggregates = new ArrayList<>();

        public AggregateValue(List<Value> aggregates) {
            this.aggregates = new ArrayList<>(aggregates);
        }

        public Value get(int i) {
            return aggregates.get(i);
        }

        public void set(int i, Value v) {
            aggregates.set(i, v);
        }

        public int size() {
            return aggregates.size();
        }
    }

    ;

    /**
     * A simplified hash table that has all the necessary functionality for aggregations.
     */
    class SimpleAggregationHashTable {

        private List<AbstractExpression> aggExprs;
        private List<AggregationType> aggTypes;
        private Map<AggregateKey, AggregateValue> ht;

        /**
         * Create a new simplified aggregation hash table.
         *
         * @param agg_exprs the aggregation expressions
         * @param agg_types the types of aggregations
         */
        public SimpleAggregationHashTable(List<AbstractExpression> aggExprs,
                                          List<AggregationType> aggTypes) {
            this.aggExprs = aggExprs;
            this.aggTypes = aggTypes;
        }

        /**
         * @return the initial aggregrate value for this aggregation executor
         */
        public AggregateValue generateInitialAggregateValue() {
            List<Value> values = new ArrayList<>();
            for (AggregationType aggType : aggTypes) {
                switch (aggType) {
                    case CountAggregate:
                        // Count starts at zero.
                        values.add(new Value(0));
                        break;
                    case SumAggregate:
                        // Sum starts at zero.
                        values.add(new Value(0));
                        break;
                    case MinAggregate:
                        // Min starts at INT_MAX.
                        values.add(new Value(Integer.MAX_VALUE));
                        break;
                    case MaxAggregate:
                        // Max starts at INT_MIN.
                        values.add(new Value(Integer.MIN_VALUE));
                        break;
                }
            }
            return new AggregateValue(values);
        }

        /**
         * Combines the input into the aggregation result.
         */
        public void combineAggregateValues(AggregateValue result, AggregateValue input) {
            for (int i = 0; i < aggExprs.size(); i++) {
                switch (aggTypes.get(i)) {
                    case CountAggregate:
                        // Count increases by one.
                        result.set(i, result.get(i).add(new Value(1)));
                        break;
                    case SumAggregate:
                        // Sum increases by addition.
                        result.set(i, result.get(i).add(new Value(input.get(i))));
                        break;
                    case MinAggregate:
                        // Min is just the min.
                        result.set(i, result.get(i).min(new Value(input.get(i))));
                        break;
                    case MaxAggregate:
                        // Max is just the max.
                        result.set(i, result.get(i).max(new Value(input.get(i))));
                        break;
                }
            }
        }

        /**
         * Inserts a value into the hash table and then combines it with the current aggregation.
         *
         * @param agg_key the key to be inserted
         * @param agg_val the value to be inserted
         */
        public void insertCombine(AggregateKey aggKey, AggregateValue aggVal) {
            if (ht.get(aggKey) == null) {
                ht.put(aggKey, generateInitialAggregateValue());
            }
            combineAggregateValues(ht.get(aggKey), aggVal);
        }

        /**
         * @return iterator to the start of the hash table
         */
        Iterator begin() {
            return ht.entrySet().iterator();
        }

        public void put(AggregateKey key, AggregateValue value) {
        }
    }

    /**
     * The aggregation plan node.
     */
    private AggregationPlanNode plan;
    /**
     * The child executor whose tuples we are aggregating.
     */
    private AbstractExecutor child;
    /**
     * Simple aggregation hash table.
     */
    private SimpleAggregationHashTable aht;
    /**
     * Simple aggregation hash table iterator.
     */
    private Iterator ahtIterator;


    public AggregationExecutor(ExecutorContext execCtx, AggregationPlanNode plan, AbstractExecutor child) {
        super(execCtx);
        this.plan = plan;
        this.child = child;
    }

    /**
     * Do not use or remove this function, otherwise you will get zero points.
     */
    public AbstractExecutor getChildExecutor(){
        return child;
    }

    public Schema getOutputSchema(){
        return plan.getOutputSchema();
    }

    @Override
    public void init() {
        this.child.init();
        Tuple[] tuple = new Tuple[1];
        while (this.child.next(tuple)) {
            AggregateKey key = this.makeKey(tuple[0]);
            AggregateValue value = this.makeVal(tuple[0]);
            this.aht.insertCombine(key, value);
        }
        this.ahtIterator = this.aht.begin();
    }

    @Override
    public boolean next(Tuple[] tuple){
        while (this.ahtIterator.hasNext()) {
            Map.Entry<AggregateKey, AggregateValue> entry = (Map.Entry<AggregateKey, AggregateValue>) this.ahtIterator.next();
            AggregateKey key = entry.getKey();
            AggregateValue val = entry.getValue();
            if ((this.plan.getHaving() == null) ||
            (this.plan.getHaving().evaluateAggregate(key.groupBys, val.aggregates).getAsBoolean())) {
                List<Value> result = new ArrayList<>();
                for ( column : this.getOutputSchema().getColumns() ) {
                    result.add(column.getExpr().evaluateAggregate(key.groupBys, val.aggregates));
                }
            tuple[0] = new Tuple(result, this.getOutputSchema());
                return true;
            }
        }
        return false;
    }

    /**
     * @return the tuple as an AggregateKey
     */
    public AggregateKey makeKey(Tuple tuple) {
        List<Value> keys = new ArrayList<>();
        for (AbstractExpression expr : plan.getGroupBys()) {
            keys.add(((AggregateValueExpression)expr).evaluate(tuple, child.getOutputSchema()));
        }
        return new AggregateKey(keys);
    }

    /**
     * @return the tuple as an AggregateValue
     */
    public AggregateValue makeVal(Tuple tuple) {
        List<Value> vals = new ArrayList<>();
        for (AbstractExpression expr : plan.getGroupBys()) {
            vals.add(((AggregateValueExpression)expr).evaluate(tuple, child.getOutputSchema()));
        }
        return new AggregateValue(vals);
    }

}