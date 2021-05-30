package main.execution.queryExecutor;

import main.execution.ExecutorContext;
import main.execution.executors.AbstractExecutor;
import main.execution.expressions.AbstractExpression;
import main.execution.expressions.AggregateValueExpression;
import main.execution.plans.AggregationPlanNode;
import main.execution.plans.AggregationType;
import main.storage.table.Tuple;
import main.type.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class ExecuorAgg {
    class AggKey {
        List<Value> groupBys;

        public AggKey(List<Value> groupBys) {
            this.groupBys = new ArrayList<>(groupBys);
        }

        public boolean equals(AggKey other) {
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

    class SimpleAggregationHashTable {

        private List<AbstractExpression> aggExprs;
        private List<AggregationType> aggTypes;
        private Map<AggKey, AggregateValue> ht;


        public SimpleAggregationHashTable(List<AbstractExpression> aggExprs,
                                          List<AggregationType> aggTypes) {
            this.aggExprs = aggExprs;
            this.aggTypes = aggTypes;
        }


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


        Iterator begin() {
            return ht.entrySet().iterator();
        }

        public void put(AggKey key, AggregateValue value) {
        }
    }


    private AggregationPlanNode plan;

    private AbstractExecutor child;

    private SimpleAggregationHashTable aht;

    private Iterator ahtIterator;

    public ExecuorAgg() {
    }

    public ExecuorAgg(ExecutorContext execCtx, AggregationPlanNode plan, AbstractExecutor child) {
        this.plan = plan;
        this.child = child;
    }

    public AbstractExecutor getChildExecutor(){
        return child;
    }



    public void init() {
        this.child.init();
        Tuple[] tuple = new Tuple[1];
        while (this.child.next(tuple)) {
            AggKey key = this.makeKey(tuple[0]);
            AggregateValue value = this.makeVal(tuple[0]);
        }
        this.ahtIterator = this.aht.begin();
    }

    public boolean next(Tuple[] tuple){
        while (this.ahtIterator.hasNext()) {
            Map.Entry<AggKey, AggregateValue> entry = (Map.Entry<AggKey, AggregateValue>) this.ahtIterator.next();
            AggKey key = entry.getKey();
            AggregateValue val = entry.getValue();
            if ((this.plan.getHaving() == null) ||
                    (this.plan.getHaving().evaluateAggregate(key.groupBys, val.aggregates).getAsBoolean())) {
                List<Value> result = new ArrayList<>();
                return true;
            }
        }
        return false;
    }

    public AggKey makeKey(Tuple tuple) {
        List<Value> keys = new ArrayList<>();
        for (AbstractExpression expr : plan.getGroupBys()) {
            keys.add(((AggregateValueExpression)expr).evaluate(tuple, child.getOutputSchema()));
        }
        return new AggKey(keys);
    }


    public AggregateValue makeVal(Tuple tuple) {
        List<Value> vals = new ArrayList<>();
        for (AbstractExpression expr : plan.getGroupBys()) {
            vals.add(((AggregateValueExpression)expr).evaluate(tuple, child.getOutputSchema()));
        }
        return new AggregateValue(vals);
    }
}
