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

public class AdrouterData extends Utilities implements DataStrategyInterface  {

	@Override
	public DataFrame funcAuditParsing(JavaRDD<?> lines,SQLContext sqlContext, Properties colProp) {
        System.out.println("............. getRowsFromAdrAudits...........");
        
        final int rows=32; // ################### no of rows
        
        JavaRDD<Row> l2 = lines.flatMap(s-> ToListOfRows.AdrToListOfRows(s, colProp));
        
    //############################## FLAT MAP ENDS############################################################################################   
        
        System.out.println("L2  "+l2);
        l2.collect();
        System.out.println("L2 -- COUNT --- "+l2.count());
        
        JavaRDD<Row> jj=l2;
        
       
        
        System.out.println(".........inside....getDataFrameFromAdrouterRows.......");
        ArrayList<StructField> fields = new ArrayList<StructField>();
              
        for(int s2=1;s2<=rows;s2++){
        	fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col"+s2), (DataType)DataTypes.StringType, (boolean)true));
        }
        
        StructType schema = DataTypes.createStructType(fields);
        DataFrame df1 = sqlContext.createDataFrame(jj, schema); // DATAFRAME Generated ###################################
        
      // df1.show();
	     
	 return df1;
    }

	/*@Override
	public DataFrame funcAuditTable(JavaRDD<Row> rowRDD, SQLContext sqlContext, Properties colProp) {
        return null;
    }
	*/
	

}
