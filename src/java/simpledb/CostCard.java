package simpledb;
import java.util.Vector;

/** Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the
    cost and cardinality of the optimal plan represented by plan.
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost;
    /** The cardinality of the optimal subplan */
    public int card;
    /** The optimal subplan */
    public Vector<LogicalJoinNode> plan;

    public CostCard() {}

    public CostCard(double cost, int card, Vector<LogicalJoinNode> plan) {
        this.cost = cost;
        this.card = card;
        this.plan = plan;
    }

    // this object denotes an impossible plan
    public static final CostCard IMPOSSIBLE = new CostCard(Double.MAX_VALUE, 0, new Vector<>());
}
