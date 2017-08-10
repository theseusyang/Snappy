import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.*;



public class JSON2Table {
public static void main(String args[]) throws SQLException{
    
	// 初始化数据加载
	
	try {
		Class.forName("io.snappydata.jdbc.ClientDriver");
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	Connection conn =
    	      DriverManager.getConnection("jdbc:snappydata://127.0.0.1[1527]");
	
    Statement st = conn.createStatement();
//    st.execute("CREATE TABLE");
//    st.execute("INSERT INTO temp VALUES(1)");
//    st.execute("CREATE EXTERNAL TABLE T1 USING parquet OPTIONS(path 'hdfs://localhost:81/test/NYSE.txt');");
    
    String dropTableStr = "DROP TABLE IF EXISTS people ;";
    	String createTableStr =  "CREATE TABLE"+" people "+" (id integer, name varchar(100),taxnumber integer )";
    
    st.execute(dropTableStr);
    st.execute(createTableStr);
    
    
    
    String insertSQL = "insert into people values(?,?,?)";
    
    PreparedStatement pStat = conn.prepareStatement(insertSQL);
    
    
    String json="[{\"Id\":\"1\",\"Name\":\"张三\",\"TaxNumber\":98320},{\"Id\":\"2\",\"Name\":\"李四\",\"TaxNumber\":8943530}]";
    List<People> peopleList = JSONObject.parseArray(json,People.class);
    for(int i =0 ; i < peopleList.size(); i++){
    		People people = (People)peopleList.get(i);
    		pStat.setInt(1, people.getId());
    		pStat.setString(2, people.getName());
    		pStat.setLong(3, people.getTaxNumber());
    }
    pStat.execute();
    
    ResultSet rs = st.executeQuery("select * from people");
   
    while(rs.next()){
    		System.out.println(rs.getInt(1));
    		System.out.println(rs.getString(2));
    		System.out.println(rs.getLong(3));
    }
//    int id = people.getId();
    
    pStat.close();
    rs.close();
}
}
