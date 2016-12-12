package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class ToListOfRows{

	public static ArrayList<Row> AdrToListOfRows(Object s, Properties colProp) {
		final int rows = 32;

		HashMap<Integer, String> OneRowMap = new HashMap<Integer, String>();
		HashMap<Integer, String> hMap = new HashMap<Integer, String>();
		HashMap<Integer, String> rowMap = new HashMap<Integer, String>();
		ArrayList<Row> lRow = new ArrayList<Row>();

		String[] arr1 = ((String) s).split("\\^");
		int count = 0;
		while (count < arr1.length) {
			hMap.put(count, arr1[count]);
			System.out.println(" after ^  " + (String) hMap.get(count));
			++count;
		}
		System.out.println(" SIZE ... ... " + hMap.size());
		if (hMap.size() == 1) {
			System.out.println();
			hMap.put(1, "#ACCOUNT DUMMY_VALUE#ADCOUNT DUMMY_VALUE");
		}
		int com = 0;
		while (com < hMap.size() - 1) {
			rowMap.put(com, String.valueOf((String) hMap.get(0)) + "#" + (String) hMap.get(com + 1));
			++com;
		}
		int one = 0;
		int rr = 0;
		while (rr < rowMap.size()) {
			String[] temp = ((String) rowMap.get(rr)).split("#");
			int inner = 0;
			while (inner < temp.length) {
				if (temp[inner].equals(null) || temp[inner].equals("") || temp[inner].equals("*")
						|| temp[inner].isEmpty()) {
					temp[inner] = "DUMMY VALUE";
				}
				++inner;
			}
			int check = 0;
			while (check < temp.length) {
				System.out.println(" this is temp  ----  " + temp[check]);
				++check;
			}
			String result = "";
			String res1 = "";
			int pr = 0;
			while (pr < temp.length) {
				if (!temp[pr].matches("^ADCOUNT.*")) {
					result = String.valueOf(result) + "," + temp[pr];
				} else {
					res1 = String.valueOf(result) + "," + temp[pr];
				}
				if (!res1.isEmpty()) {
					OneRowMap.put(one, res1);
					++one;
				}
				++pr;
			}
			++rr;
		}
		OneRowMap.forEach((k, v) -> {
			System.out.println(" CommonPart+ADCOUNTS together -- OneRowMap   " + k + " " + v);
		});
		String[] rpair = null;
		int lcount = 0;
		while (lcount < OneRowMap.size()) {
			Object[] rowArray = new Object[rows]; // ##########################
													// no of columns
													// ##################
			HashMap<String, String> rowMap2 = new HashMap<String, String>();
			System.out.println(" ====================  CATCH ========rpair =====  " + OneRowMap);
			rpair = ((String) OneRowMap.get(lcount)).split(",");
			String[] val = new String[] { "one", "two" };
			System.out.println(" ====================  CATCH ========rpair =====  " + rpair);
			int last = 2;
			while (last < rpair.length) {
				String ret;
				Utilities u;
				System.out.println(" ====================  CATCH ========rpair[last] =====  " + rpair[last]);
				val = rpair[last].split(" ");
				if (val.length == 1) {
					val[0] = String.valueOf(val[0]) + " DUMMYVALUE";
					val = val[0].split(" ");
				}
				rowMap2.put(val[0], val[1]);
				String[] arr11 = null;
				if (val[0].equals("EVENTTIME")) {
					u = new Utilities();
					ret = u.getLocalDateID(val[1], "America/New_York");
					rowMap2.put("EVENTTIME_EST", ret);
				}
				if (val[0].equals("LOGTIME")) {
					u = new Utilities();
					ret = u.getLocalDateID(val[1], "America/Los_Angeles");
					rowMap2.put("LOGTIME_PST", ret);
				}
				if (val[0].equals("ASSETINFO")) {
					arr11 = val[1].split(":");
					int j1 = 0;
					while (j1 < arr11.length) {
						if (arr11[j1].equals("ASSETID")) {
							rowMap2.put("ASSETID_Cr", arr11[j1 + 1]);
						}
						if (arr11[j1].equals("DATABASEID")) {
							rowMap2.put("DATABASEID_Cr", arr11[j1 + 1]);
						}
						if (arr11[j1].equals("NAME")) {
							rowMap2.put("NAME_Cr", arr11[j1 + 1]);
						}
						++j1;
					}
				}

				if (val[0].equals("PROGRAMPOSITION")) {
					if (val[1].equals("37")) {
						rowMap2.put("PROGRAMPOSITION", "Pre");
					} else if (val[1].equals("38")) {
						rowMap2.put("PROGRAMPOSITION", "Mid");
					} else if (val[1].equals("39")) {
						rowMap2.put("PROGRAMPOSITION", "Post");
					}

				}

				rowArray[0] = rowMap2.get(colProp.getProperty("adr_col1"));
				rowArray[1] = rowMap2.get(colProp.getProperty("adr_col2"));
				rowArray[2] = rowMap2.get(colProp.getProperty("adr_col3"));
				rowArray[3] = rowMap2.get(colProp.getProperty("adr_col4"));
				rowArray[4] = rowMap2.get(colProp.getProperty("adr_col5"));
				rowArray[5] = rowMap2.get(colProp.getProperty("adr_col6"));
				rowArray[6] = rowMap2.get(colProp.getProperty("adr_col7"));
				rowArray[7] = rowMap2.get(colProp.getProperty("adr_col8"));
				rowArray[8] = rowMap2.get(colProp.getProperty("adr_col9"));
				rowArray[9] = rowMap2.get(colProp.getProperty("adr_col10"));
				rowArray[10] = rowMap2.get(colProp.getProperty("adr_col11"));
				rowArray[11] = rowMap2.get(colProp.getProperty("adr_col12"));
				rowArray[12] = rowMap2.get(colProp.getProperty("adr_col13"));
				rowArray[13] = rowMap2.get(colProp.getProperty("adr_col14"));
				rowArray[14] = rowMap2.get(colProp.getProperty("adr_col15"));
				rowArray[15] = rowMap2.get(colProp.getProperty("adr_col16"));
				rowArray[16] = rowMap2.get(colProp.getProperty("adr_col17"));
				rowArray[17] = rowMap2.get(colProp.getProperty("adr_col18"));
				rowArray[18] = rowMap2.get(colProp.getProperty("adr_col19"));
				rowArray[19] = rowMap2.get(colProp.getProperty("adr_col20"));
				rowArray[20] = rowMap2.get(colProp.getProperty("adr_col21"));
				rowArray[21] = rowMap2.get(colProp.getProperty("adr_col22"));
				rowArray[22] = rowMap2.get(colProp.getProperty("adr_col23"));
				rowArray[23] = rowMap2.get(colProp.getProperty("adr_col24"));
				rowArray[24] = rowMap2.get(colProp.getProperty("adr_col25"));
				rowArray[25] = rowMap2.get(colProp.getProperty("adr_col26"));
				rowArray[26] = rowMap2.get(colProp.getProperty("adr_col27"));
				rowArray[27] = rowMap2.get(colProp.getProperty("adr_col28"));

				rowArray[28] = rowMap2.get(colProp.getProperty("adr_col29"));
				rowArray[29] = rowMap2.get(colProp.getProperty("adr_col30"));
				rowArray[30] = rowMap2.get(colProp.getProperty("adr_col31"));
				rowArray[31] = rowMap2.get(colProp.getProperty("adr_col32"));

				long l = System.currentTimeMillis();
				String ss = Long.toString(l);
				System.out.println("POMPU 007 " + ss);
				Utilities u1 = new Utilities();
				String dateUpdated_pst = u1.getLocalDateID_All(ss, "America/Los_Angeles");
				rowArray[31] = dateUpdated_pst;
				last++;
			}
			Row aRow = RowFactory.create((Object[]) rowArray);
			lRow.add(aRow);
			lcount++;
		}

		// System.out.println("ASSAM --- "+lRow);
		return lRow;

	}
	
	
	public static ArrayList<Row> FeedToListOfRows(Object s, Properties colProp) {

		final int rows = 8;
        Object[] rowArray = new Object[rows];
        Row aRow = RowFactory.create((Object[])rowArray);
        String[] arr = ((String) s).split(",");  // First Split of line 
        
        
        for(int ex=0;ex<arr.length;ex++){
        	System.out.println("ex ex ex PPPPPPPPPPPPPPPPPPPPPP-  "+ ex +arr[ex]);
        }
        
        
	 ArrayList<Row> lRow = new ArrayList<Row>();
	 
	 HashMap<String, String> rowMap2 = new HashMap<String, String>();
        String[] pair = arr[0].split(":");
        Utilities u = new Utilities();
        String ret = u.getLocalDateID(pair[1], "America/New_York");
        rowMap2.put("PSN_LOGTIME_PST", ret);
        rowArray[0] = rowMap2.get(colProp.getProperty("feed_col1"));
        int k = 1;
        while (k < arr.length) {
            rowMap2.put("PSN_ASSETID_Cr", arr[1]);
            rowMap2.put("PSN_DURATION", arr[6]);
            rowMap2.put("PSN_EVENT", arr[2]);
            String ret1 = u.getLocalDateID(arr[3], "America/New_York");
            rowMap2.put("PSN_EVENTTIME_EST", ret1);
            rowMap2.put("PSN_NPT", arr[4]);
            rowMap2.put("PSN_SCALE", arr[5]);
            rowArray[1] = rowMap2.get(colProp.getProperty("feed_col2"));
            rowArray[2] = rowMap2.get(colProp.getProperty("feed_col3"));
            rowArray[3] = rowMap2.get(colProp.getProperty("feed_col4"));
            rowArray[4] = rowMap2.get(colProp.getProperty("feed_col5"));
            rowArray[5] = rowMap2.get(colProp.getProperty("feed_col6"));
            rowArray[6] = rowMap2.get(colProp.getProperty("feed_col7"));
            
            long l = System.currentTimeMillis();
            String ss1 = Long.toString(l);
            System.out.println("POMPU 007 " + ss1);
            Utilities u2 = new Utilities();
            String dateUpdated_pst = u2.getLocalDateID_All(ss1, "America/Los_Angeles");
            rowArray[7] = dateUpdated_pst;
            
            k++;
        }
        lRow.add(aRow);
        System.out.println("pom pom " + lRow);
        		 
	 
	 
        
	 return lRow; 
	}

}
