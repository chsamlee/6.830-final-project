package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class CustomQueryTest {

    /*
    @Test(timeout=20000) public void queryTest1() throws IOException, DbException, TransactionAbortedException {
        final int IO_COST = 101;

        // Create all of the tables, and add them to the catalog
        ArrayList<ArrayList<Integer>> aTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile emp = SystemTestUtil.createRandomHeapFile(
                6, 100, 0, 10000, null, aTuples, "a");
        Database.getCatalog().addTable(emp, "A");

        ArrayList<ArrayList<Integer>> bTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile dept = SystemTestUtil.createRandomHeapFile(
                3, 10000, 9000, 19000, null, bTuples, "b");
        Database.getCatalog().addTable(dept, "B");

        // Get TableStats objects for each of the tables that we just generated.
        TableStats.setTableStats("A", new TableStats(Database.getCatalog().getTableId("A"), IO_COST));
        TableStats.setTableStats("B", new TableStats(Database.getCatalog().getTableId("B"), IO_COST));

        Transaction t = new Transaction();
        t.start();
        Parser p = new Parser();
        p.setTransaction(t);
        p.processNextStatement("SELECT * FROM A,B WHERE A.a1 = B.b1;");
    }
    */

    @Test(timeout=20000) public void queryTest2() throws IOException, DbException, TransactionAbortedException {
        final int IO_COST = 101;

        // Create all of the tables, and add them to the catalog
        ArrayList<ArrayList<Integer>> aTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile emp = SystemTestUtil.createRandomHeapFile(
                6, 200, 100, 1000, null, aTuples, "a");
        Database.getCatalog().addTable(emp, "A");

        ArrayList<ArrayList<Integer>> bTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile dept = SystemTestUtil.createRandomHeapFile(
                3, 500, 900, 1900, null, bTuples, "b");
        Database.getCatalog().addTable(dept, "B");

        ArrayList<ArrayList<Integer>> cTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobby = SystemTestUtil.createRandomHeapFile(
                5, 500, 500, 1500,null, cTuples, "c");
        Database.getCatalog().addTable(hobby, "C");

        ArrayList<ArrayList<Integer>> dTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobbies = SystemTestUtil.createRandomHeapFile(
                4, 100, 1400, 2400,null, dTuples, "d");
        Database.getCatalog().addTable(hobbies, "D");

        // Get TableStats objects for each of the tables that we just generated.
        TableStats.setTableStats("A", new TableStats(Database.getCatalog().getTableId("A"), IO_COST));
        TableStats.setTableStats("B", new TableStats(Database.getCatalog().getTableId("B"), IO_COST));
        TableStats.setTableStats("C", new TableStats(Database.getCatalog().getTableId("C"), IO_COST));
        TableStats.setTableStats("D", new TableStats(Database.getCatalog().getTableId("D"), IO_COST));

        Transaction t = new Transaction();
        t.start();
        Parser p = new Parser();
        p.setTransaction(t);
        // p.processNextStatement("SELECT * FROM A,B WHERE A.a1 = B.b1;");
        p.processNextStatement("SELECT * FROM A,B,C,D WHERE A.a1 = B.b1 AND B.b2 > C.c2 AND C.c3 = D.d3;");
    }

}
