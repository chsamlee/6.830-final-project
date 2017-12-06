package simpledb;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DummyTest {

    @BeforeClass
    public static void setUpClass() {
        System.out.println("Called before any test is run");
        /**
         * Create table here
         * 1. have an arraylist of data
         * 2. call convert on the arraylist with outFile = temp file
         * 3. add temp file into catalog
         */
        /**
         * Cleanup
         * 1. delete table from catalog
         * 2. delete temp file (may be auto)
         */
    }

    @Before
    public void setUp() {
        System.out.println("Called every time before a test is run");
    }

    @Test
    public void testOne() {
        assertTrue(1 + 1 > 1);
    }

    @Test
    public void testTwo() {
        assertTrue(1 + 1 == 2);
    }

}
