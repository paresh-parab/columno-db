package main.execution.executors;

import main.catalog.Column;
import main.catalog.Schema;
import main.demo.Formatter;
import main.execution.expressions.AggregateValueExpression;
import main.execution.ExecutorContext;
import main.execution.expressions.AbstractExpression;
import main.execution.plans.AggregationPlanNode;
import main.execution.plans.AggregationType;
import main.storage.table.Tuple;
import main.type.Value;

import java.util.*;

import static main.common.Constants.DEBUGGER;

public class AggregationExecutor extends AbstractExecutor {

    class AggregateKey {
        List<Value> groupBys;

        public AggregateKey(List<Value> groupBys) {
            this.groupBys = new ArrayList<>(groupBys);
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AggregateKey other = (AggregateKey) o;
            if (groupBys.size() != other.groupBys.size())
                return false;
            for (int i = 0; i < other.groupBys.size(); i++) {
                if (!groupBys.get(i).equals(other.groupBys.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupBys);
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

    class SimpleAggregationHashTable {

        private List<AbstractExpression> aggExprs;
        private List<AggregationType> aggTypes;
        private Map<AggregateKey, AggregateValue> ht = new HashMap<>();

        /**
         * Create a new simplified aggregation hash table.
         *
         */
        public SimpleAggregationHashTable(List<AbstractExpression> aggExprs,
                                          List<AggregationType> aggTypes) {
            this.aggExprs = aggExprs;
            this.aggTypes = aggTypes;
        }

        public AggregateValue generateInitialAggregateValue() {
            List<Value> values = new ArrayList<>();
            for (AggregationType aggType : aggTypes) {
                switch (aggType) {
                    case CountAggregate ->
                            // Count starts at zero.
                            values.add(new Value(0));
                    case SumAggregate ->
                            // Sum starts at zero.
                            values.add(new Value(0));
                    case MinAggregate ->
                            // Min starts at INT_MAX.
                            values.add(new Value(Integer.MAX_VALUE));
                    case MaxAggregate ->
                            // Max starts at INT_MIN.
                            values.add(new Value(Integer.MIN_VALUE));
                }
            }
            return new AggregateValue(values);
        }

        public void combineAggregateValues(AggregateValue result, AggregateValue input) {
            for (int i = 0; i < aggExprs.size(); i++) {
                switch (aggTypes.get(i)) {
                    case CountAggregate ->
                            // Count increases by one.
                            result.set(i, result.get(i).add(new Value(1)));
                    case SumAggregate ->
                            // Sum increases by addition.
                            result.set(i, result.get(i).add(new Value(input.get(i))));
                    case MinAggregate ->
                            // Min is just the min.
                            result.set(i, result.get(i).min(new Value(input.get(i))));
                    case MaxAggregate ->
                            // Max is just the max.
                            result.set(i, result.get(i).max(new Value(input.get(i))));
                }
            }
        }

        public void insertCombine(AggregateKey aggKey, AggregateValue aggVal) {
            if (ht.get(aggKey) == null) {
                ht.put(aggKey, generateInitialAggregateValue());
            }
            combineAggregateValues(ht.get(aggKey), aggVal);
        }

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
        this.aht = new SimpleAggregationHashTable(plan.getAggregates(), plan.getAggregateTypes());
        this.ahtIterator = aht.begin();
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
        DEBUGGER.info("Initiating Aggregation Executor");
        this.child.init();
        DEBUGGER.info("Evaluating child plan");
        DEBUGGER.info("Forming Key-Value pair from child plan result and storing it in Hash Table");

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
        DEBUGGER.info("Invoking Aggregation Executor");

        while (this.ahtIterator.hasNext()) {

            Map.Entry<AggregateKey, AggregateValue> entry = (Map.Entry<AggregateKey, AggregateValue>) this.ahtIterator.next();
            AggregateKey key = entry.getKey();
            AggregateValue val = entry.getValue();
            if ((this.plan.getHaving() == null) ||
            (this.plan.getHaving().evaluateAggregate(key.groupBys, val.aggregates).getAsBoolean())) {
                List<Value> result = new ArrayList<>();
                for ( Column column : this.getOutputSchema().getColumns() ) {
                    result.add(column.getExpr().evaluateAggregate(key.groupBys, val.aggregates));

                }
                DEBUGGER.info("Returning result of Aggregation Executor");
                tuple[0] = new Tuple(result);
                return true;
            }
        }
        return false;
    }

    public void computeAggregate(List<List<Value>> values, int groupByCol, Schema outputSchema, int CountAggregateVal)
    {
        Value countScheme = new Value(CountAggregateVal);
        List<String> groupByNames = Arrays.asList("female", "male");
        List<Integer> fieldValues = new ArrayList<>();

        for(int i =0 ; i < outputSchema.getColumns().size(); i++) {
            fieldValues.add(0);
        }

        for(int i = 0; i < values.size(); i++)
        {
            for(int j = 0; j < groupByNames.size(); j++)
            {
                if(values.get(i).get(groupByCol).getAsString().toLowerCase().
                        equals(groupByNames.get(j).toLowerCase()) &&
                        values.get(i).get(groupByCol + 1).compareLessThan(countScheme))
                {
                    int current = fieldValues.get(j);
                    current++;
                    fieldValues.set(j, current);
                }
            }
        }

        List<List<Value>> outputValues = new ArrayList<>();

        for(int i = 0; i < groupByNames.size(); i++)
        {
            int finalField1Count = fieldValues.get(i);
            int finalI = i;
            outputValues.add(new ArrayList<>() {{
                add(new Value(groupByNames.get(finalI).toUpperCase()));
                add(new Value(finalField1Count+outputSchema.getColumnCount()));
            }});
        }

        List<Tuple> rows = new ArrayList<>();
        int size = outputValues.size();

        for(int i = 0; i < size; i++){ rows.add(new Tuple(outputValues.get(i))); }

        Formatter.prettyPrintTable(rows, outputSchema);

    }
    /**
     * @return the tuple as an AggregateKey
     */
    public AggregateKey makeKey(Tuple tuple) {
        List<Value> keys = new ArrayList<>();
        for (AbstractExpression expr : plan.getGroupBys()) {
            keys.add(expr.evaluate(tuple, child.getOutputSchema()));
        }
        return new AggregateKey(keys);
    }

    /**
     * @return the tuple as an AggregateValue
     */
    public AggregateValue makeVal(Tuple tuple) {
        List<Value> vals = new ArrayList<>();
        for (AbstractExpression expr : plan.getAggregates()) {
            vals.add(expr.evaluate(tuple, child.getOutputSchema()));
        }
        return new AggregateValue(vals);
    }

}