package simpledb;

import java.util.*;

import javax.swing.*;
import javax.swing.tree.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    LogicalPlan p;
    Vector<LogicalJoinNode> joins;

    private static final int MAX_RIGHT_TREE_DEPTH = 3;

    /**
     * Constructor
     *
     * @param p
     *            the logical plan being optimized
     * @param joins
     *            the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, Vector<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     *
     * @param lj
     *            The join being considered
     * @param plan1
     *            The left join node's child
     * @param plan2
     *            The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj,
                                             OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        j = new Join(p,plan1,plan2);

        return j;

    }

    /**
     * Estimate the cost of a join.
     *
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     *
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Estimated cardinality of the left-hand side of the query
     * @param card2
     *            Estimated cardinality of the right-hand side of the query
     * @param cost1
     *            Estimated cost of one full scan of the table on the left-hand
     *            side of the query
     * @param cost2
     *            Estimated cost of one full scan of the table on the right-hand
     *            side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
            double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     *
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Cardinality of the left-hand table in the join
     * @param card2
     *            Cardinality of the right-hand table in the join
     * @param t1pkey
     *            Is the left-hand table a primary-key table?
     * @param t2pkey
     *            Is the right-hand table a primary-key table?
     * @param stats
     *            The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
            boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     * */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
            String table1Alias, String table2Alias, String field1PureName,
            String field2PureName, int card1, int card2, boolean t1pkey,
            boolean t2pkey, Map<String, TableStats> stats,
            Map<String, Integer> tableAliasToId) {
        // if we have stats, probe into IntHistogram for a more accurate estimate
        if (stats.containsKey(table1Alias) && stats.containsKey(table2Alias)) {
            int baseCard;
            if (joinOp == Predicate.Op.EQUALS) {
                if (t1pkey) {
                    baseCard = card2;
                }
                else if (t2pkey) {
                    baseCard = card1;
                }
                else {
                    baseCard = card1 * card2;
                }
            } else {
                baseCard = card1 * card2;
            }
            double selectivity = TableStats.estimateJoinSelectivity(
                    stats.get(table1Alias),
                    field1PureName,
                    stats.get(table2Alias),
                    field2PureName,
                    joinOp
            );
            return (int) (selectivity * baseCard);
        }
        // estimateJoinCardinality test in JoinOptimizerTest doesn't come with
        // stats so we have to keep the simple solution
        if (joinOp == Predicate.Op.EQUALS) {
            if (t1pkey) {
                return card2;
            }
            else if (t2pkey) {
                return card1;
            }
            else {
                return Math.max(card1, card2);
            }
        }
        return (int) (0.7 * card1 * card2);
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.
     *
     * @param stats
     *            Statistics for each table involved in the join, referenced by
     *            base table names, not alias
     * @param filterSelectivities
     *            Selectivities of the filter predicates on each table in the
     *            join, referenced by table alias (if no alias, the base table
     *            name)
     * @param explain
     *            Indicates whether your code should explain its query plan or
     *            simply execute it
     * @return A Vector<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     * @throws ParsingException
     *             when stats or filter selectivities is missing a table in the
     *             join, or or when another internal error occurs
     */
    public Vector<LogicalJoinNode> orderJoins(
            HashMap<String, TableStats> stats,
            HashMap<String, Double> filterSelectivities, boolean explain)
            throws ParsingException {
        // build a bidirectional mapping between tables and bits
        Map<String, Integer> tablesToBits = new HashMap<>();
        Map<Integer, String> bitsToTables = new HashMap<>();
        for (LogicalJoinNode j: joins) {
            for (String alias: new String[]{j.t1Alias, j.t2Alias}) {
                if (alias != null && !tablesToBits.containsKey(alias)) {
                    int bit = tablesToBits.size();
                    tablesToBits.put(alias, bit);
                    bitsToTables.put(bit, alias);
                }
            }
        }
        // base case: 1 table
        Map<Long, CostCard> planCache = new HashMap<>();
        for (int i = 0; i < tablesToBits.size(); i++) {
            String table = bitsToTables.get(i);
            double cost = stats.get(table).estimateScanCost();
            int card = stats.get(table).estimateTableCardinality(filterSelectivities.get(table));
            CostCard cc = new CostCard(cost, card, new Vector<>());
            planCache.put(1L << i, cc);
        }
        // use dynamic programming to compute joins with more tables
        final int TABLE_COUNT = tablesToBits.size();
        for (int k = 2; k <= TABLE_COUNT; k++) {
            Iterator<Long> keys = kOneBitNumbers(decomposeToBits((1L << TABLE_COUNT) - 1), k);
            while (keys.hasNext()) {
                long key = keys.next();
                int[] keyDecomposition = decomposeToBits(key);
                CostCard cc = CostCard.IMPOSSIBLE;
                for (int depth = 1; depth <= Math.min(keyDecomposition.length , MAX_RIGHT_TREE_DEPTH); depth++) {
                    Iterator<Long> subsetKeyIter = kOneBitNumbers(keyDecomposition, depth);
                    while (subsetKeyIter.hasNext()) {
                        long subsetKey2 = subsetKeyIter.next();
                        long subsetKey1 = key ^ subsetKey2;
                        if (planCache.containsKey(subsetKey1) && planCache.containsKey(subsetKey2)) {
                            // planCache contains subsetKeyX - possible to join the subset together
                            cc = findBestPlan(
                                    subsetKey1, subsetKey2, tablesToBits,
                                    bitsToTables, planCache, stats, cc);
                        }
                    }
                }
                if (cc != CostCard.IMPOSSIBLE) {
                    planCache.put(key, cc);
                }
            }
        }
        Vector<LogicalJoinNode> plan = planCache.get((1L << TABLE_COUNT) - 1).plan;
        if (explain) {
            printJoins(plan, planCache, tablesToBits, stats, filterSelectivities);
        }
        return plan;
    }

    // ====== Helper methods for orderJoins ======

    /**
     * Decompose a number n into a list of numbers e_i such that n = sum(2 ** e_i)
     * The array elements are sorted in increasing order.
     */
    private int[] decomposeToBits(long n) {
        // first deconstruct the number into bits with value 1
        int i = 0;
        List<Integer> oneBitsList = new ArrayList<>();
        while (n != 0) {
            if ((n & 1) == 1) {
                oneBitsList.add(i);
            }
            n >>>= 1;
            i += 1;
        }
        int[] oneBitsArray = new int[oneBitsList.size()];
        for (int j = 0; j < oneBitsList.size(); j++) {
            oneBitsArray[j] = oneBitsList.get(j);
        }
        return oneBitsArray;
    }

    /**
     * Returns an iterator of numbers with k bits equal to 1 based on the
     * bit decomposition of a number.
     */
    private Iterator<Long> kOneBitNumbers(int[] decomposition, int k) {
        if (decomposition.length < k) {
            throw new IllegalArgumentException("k is too large");
        }
        // use an array to represent combinations of bits
        int[] oneIndices = new int[k];
        for (int i = 0; i < k; i++) {
            oneIndices[i] = decomposition.length - k + i;
        }
        // compute the last number that we should ever return
        // used as an easy check for stopping
        final long lastNumToReturn = Arrays.stream(decomposition)
                                           .limit(k)
                                           .mapToLong(n -> 1L << n)
                                           .sum();

        return new Iterator<Long>() {
            long lastReturned = 0;

            @Override
            public boolean hasNext() {
                return lastReturned != lastNumToReturn;
            }

            @Override
            public Long next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (lastReturned != 0) {
                    // find the right bit to decrement
                    int decrBit = -1;
                    for (int i = 0; i < k; i++) {
                        if (oneIndices[i] != i) {
                            decrBit = i;
                            break;
                        }
                    }
                    // decrement all lower bits
                    oneIndices[decrBit]--;
                    for (int j = 0; j < decrBit; j++) {
                        oneIndices[j] = oneIndices[decrBit] - (decrBit - j);
                    }
                }
                // construct the number from the bits
                long next = 0;
                for (int i: oneIndices) {
                    next |= (1 << decomposition[i]);
                }
                lastReturned = next;
                return next;
            }
        };
    }

    /**
     * Find the optimal way to join newTableBit to subplan
     * @param outerTables [long] key corresponding to tables to go to outer loop
     * @param innerTables [long] key corresponding to tables to go to inner loop
     * @param tablesToBits [map] mapping from table names to bits
     * @param bitsToTables [ma[] mapping from bits to table names
     * @param cache [map] previously-computed best subplans
     * @param stats [map] table stats
     * @param currentBestPlan [CostCard] lowest-cost plan for joining outerTables and newTableBit so far
     * @return CostCard instance corresponding to lowest-cost plan (can be the current plan)
     */
    // include currentBestPlan so we don't have to unnecessarily construct the plan vector
    private CostCard findBestPlan(
            long outerTables,
            long innerTables,
            Map<String, Integer> tablesToBits,
            Map<Integer, String> bitsToTables,
            Map<Long, CostCard> cache,
            HashMap<String, TableStats> stats,
            CostCard currentBestPlan
    ) {
        CostCard bestPlan = currentBestPlan;
        for (LogicalJoinNode j: joins) {
            if (j.t1Alias != null && j.t2Alias != null) {
                int b1 = tablesToBits.get(j.t1Alias);
                int b2 = tablesToBits.get(j.t2Alias);
                boolean b1InOldTables = ((outerTables | (1L << b1)) == outerTables);
                boolean b2InOldTables = ((outerTables | (1L << b2)) == outerTables);
                boolean b1InNewTables = ((innerTables | (1L << b1)) == innerTables);
                boolean b2InNewTables = ((innerTables | (1L << b2)) == innerTables);

                // canonical case: b1 in outerTables, b2 in innerTables
                if (b1InNewTables && b2InOldTables) {
                    j = j.swapInnerOuter();
                }
                // outerTables and innerTables are disjoint, so at most one clause holds
                if ((b1InNewTables && b2InOldTables) || (b2InNewTables && b1InOldTables)) {
                    CostCard cc1 = cache.get(outerTables);
                    CostCard cc2 = cache.get(innerTables);
                    double cost = estimateJoinCost(j, cc1.card, cc2.card, cc1.cost, cc2.cost);
                    if (cost < bestPlan.cost) {
                        int card = estimateJoinCardinality(
                                j, cc1.card, cc2.card,
                                isPkey(bitsToTables.get(b1), j.f1PureName),
                                isPkey(bitsToTables.get(b2), j.f2PureName),
                                stats
                        );
                        Vector<LogicalJoinNode> plan = new Vector<>(cc1.plan);
                        plan.addAll(cc2.plan);
                        plan.add(j);
                        bestPlan = new CostCard(cost, card, plan);
                    }
                }
            }
        }
        return bestPlan;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias
     *            The alias of the table in the query
     * @param field
     *            The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid = p.getTableId(tableAlias);
        String pkey = Database.getCatalog().getPrimaryKey(tid);
        return (pkey != null && pkey.equals(field));
    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js
     *            the join plan to visualize
     * @param pc
     *            the plan cache accumulated while building the optimal plan
     * @param tablesToBits
     *            a mapping from table alias to their representation bits
     * @param stats
     *            table statistics for base tables
     * @param selectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     */
    private void printJoins(
            Vector<LogicalJoinNode> js,
            Map<Long, CostCard> pc,
            Map<String, Integer> tablesToBits,
            HashMap<String, TableStats> stats,
            HashMap<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        HashMap<String, DefaultMutableTreeNode> m = new HashMap<String, DefaultMutableTreeNode>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<LogicalJoinNode>();
        long pathSoFarKey = 0;
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            for (String alias: new String[]{j.t1Alias, j.t2Alias}) {
                if (alias != null) {
                    pathSoFarKey |= (1 << tablesToBits.get(alias));
                }
            }
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.get(pathSoFarKey).cost + ", card = "
                    + pc.get(pathSoFarKey).card + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                                selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                        + " (Cost = "
                                        + stats.get(table2Name)
                                                .estimateScanCost()
                                        + ", card = "
                                        + stats.get(table2Name)
                                                .estimateTableCardinality(
                                                        selectivities
                                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();
    }

}
