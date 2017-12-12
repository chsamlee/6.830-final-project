package simpledb.json;

import simpledb.TupleDesc;
import simpledb.Type;

public class SampleData {

    public static class Table {
        public final String name;
        public final TupleDesc td;
        public final Object[][] data;

        public Table(String name, TupleDesc td, Object[][] data) {
            this.name = name;
            this.td = td;
            this.data = data;
        }
    }

    private static final TupleDesc schoolTupleDesc =
            new TupleDesc(
                    new Type[]{Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE},
                    new String[]{"id", "name", "city"}
            );

    private static final Object[][] schoolData =
            new Object[][]{
                    new Object[]{
                            0, "MIT", 1
                    },
                    new Object[]{
                            1, "Harvard", 1
                    },
                    new Object[]{
                            2, "Drexel", 6
                    },
                    new Object[]{
                            3, "UW", 0
                    }
            };

    private static final TupleDesc studentTupleDesc =
            new TupleDesc(
                    new Type[]{Type.INT_TYPE, Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE},
                    new String[]{"id", "first_name", "last_name", "year", "school_id"}
            );

    private static final Object[][] studentData =
            new Object[][]{
                    new Object[]{
                            0, "Ariel", "Jacobs", 5, 0
                    },
                    new Object[]{
                            1, "Ben", "Bitdiddle", 2, 0
                    },
                    new Object[]{
                            2, "Alicia", "Hacker", 3, 0
                    },
                    new Object[]{
                            3, "Lawyer", "McLawyerface", 1, 1
                    }
            };

    private static final TupleDesc cityTupleDesc =
            new TupleDesc(
                    new Type[]{Type.INT_TYPE, Type.STRING_TYPE, Type.STRING_TYPE},
                    new String[]{"id", "city", "state"}
            );

    private static final Object[][] cityData =
            new Object[][]{
                    new Object[]{
                            0, "Seattle", "WA"
                    },
                    new Object[]{
                            1, "Cambridge", "MA"
                    },
                    new Object[]{
                            2, "Boston", "MA"
                    },
                    new Object[]{
                            3, "New York", "NY"
                    },
                    new Object[]{
                            4, "Austin", "TX"
                    },
                    new Object[]{
                            5, "Houston", "TX"
                    },
                    new Object[]{
                            6, "Philadelphia", "PA"
                    },

            };

    public static final Table[] tables = new Table[]{
            new Table("schools", schoolTupleDesc, schoolData),
            new Table("students", studentTupleDesc, studentData),
            new Table("cities", cityTupleDesc, cityData)
    };

}