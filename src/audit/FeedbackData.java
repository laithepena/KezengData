package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FeedbackData extends Utilities implements DataStrategyInterface {

	@Override
	public DataFrame funcAuditParsing(JavaRDD<?> feeds,SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		 System.out.println("............. getRowsFromFeedBack...........");
		 final int rows=8; // ################### no of rows
		 JavaRDD<Row> l3 = feeds.flatMap(s -> ToListOfRows.FeedToListOfRows(s, colProp));
		 
	
		 //#################### Flat Map Ends ####################################################################//
		 l3.collect();
		 JavaRDD<Row> jj=l3;
		 
		 System.out.println(".......inside.....getDataFrameFromFeedbackRows");
	       // ArrayList fields = new ArrayList();
	        ArrayList<StructField> fields2 = new ArrayList<StructField>();
	        
	        
	        for(int s2=1;s2<=rows;s2++){
	        	fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col"+s2), (DataType)DataTypes.StringType, (boolean)true));
	        }
	        
	        
	     /*   fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col1"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col2"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col3"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col4"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col5"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col6"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col7"), (DataType)DataTypes.StringType, (boolean)true));*/
	        StructType schema = DataTypes.createStructType(fields2);
	        DataFrame df1 = sqlContext.createDataFrame(jj, schema);
	        return df1;
		 
		 
		 
	}

	/*@Override
	public DataFrame funcAuditTable(JavaRDD<Row> rowRDD, SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		return null;
	}*/
	
	

}
