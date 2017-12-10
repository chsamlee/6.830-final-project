package simpledb;

public class SampleData {
	static TupleDesc getSchoolTupleDesc() {
		return new TupleDesc(new Type[] {Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE}, new String[] {
				"id",
				"name",
				"city"
		});
	}
	static Object[][] getSchoolData(){
		return new Object[][] {
    		new Object[] {
    				0,"MIT", 1
    		},
    		new Object[] {
    				1,"Harvard", 1
    		},
    		new Object[] {
    				2,"Drexel", 6
    		},
    		new Object[] {
    				3,"UW", 0
    		}
		};
	}
	static TupleDesc getStudentTupleDesc() {
		return new TupleDesc(new Type[] {Type.INT_TYPE, Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE}, new String[] {
				"id",
				"first_name",
				"last_name",
				"year",
				"school_id"
		});
	}
	static Object[][] getStudentData(){
		return new Object[][] {
    		new Object[] {
    				0,"Ariel", "Jacobs", 5, 0
    		},
    		new Object[] {
    				1,"Ben",  "Bitdiddle", 2, 0
    		},
    		new Object[] {
    				2,"Alicia", "Hacker", 3, 0
    		},
    		new Object[] {
    				3,"Lawyer", "McLawyerface", 1, 1
    		}
		};
	}
	static TupleDesc getCityTupleDesc() {
		return new TupleDesc(new Type[] {Type.INT_TYPE, Type.STRING_TYPE, Type.STRING_TYPE}, new String[] {
				"id",
				"city",
				"state"
		});
	}
	static Object[][] getCityData(){
		return new Object[][] {
    		new Object[] {
    				0,"Seattle", "WA"
    		},
    		new Object[] {
    				1,"Cambridge",  "MA"
    		},
    		new Object[] {
    				2,"Boston", "MA"
    		},
    		new Object[] {
    				3,"New York", "NY"
    		},
    		new Object[] {
    				4,"Austin", "TX"
    		},
    		new Object[] {
    				5,"Houston", "TX"
    		},
    		new Object[] {
    				6,"Philadelphia", "PA"
    		},
    		
		};
	}
}