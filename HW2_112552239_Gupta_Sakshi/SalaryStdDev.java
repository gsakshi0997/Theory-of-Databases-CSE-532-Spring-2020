import java.sql.*;
import java.text.DecimalFormat;
import java.util.Formatter;
                                                        
import java.math.BigDecimal;
public class SalaryStdDev {

public static void main(String[] args)
 {
	 
   String url = "jdbc:db2://localhost:50000/"+args[0];
   String user=args[2];
   String password=args[3];
   String query = "SELECT SALARY FROM "+ args[1];
   Connection con;
   Statement stmt;
   ResultSet rs;
   double sal=0,sum=0,sq_sum=0,x_mean=0,x2_mean=0,var=0,std_d=0;
   int count=0;
   	
   if (args.length!=4)
   {
     System.err.println("Error: Missing arguments");
     System.exit(1);
   }
   try
   {                                                                        
     
     Class.forName("com.ibm.db2.jcc.DB2Driver");                            
     //System.out.println("Loaded the JDBC driver");

     con = DriverManager.getConnection (url, user, password);                
     con.setAutoCommit(false);

     stmt = con.createStatement();                                          

     rs = stmt.executeQuery(query);                  

     while (rs.next()) {
       sal = rs.getDouble(1);
       sum=sum+sal;
       sq_sum=sq_sum+(sal*sal);
       count=count+1;
       
     }
     
     rs.close();
     stmt.close();
     con.commit();
     con.close();                                                            

      x_mean=sum/count;
    	x2_mean=sq_sum/count;
    	var=x2_mean-(x_mean*x_mean);
    	std_d=Math.sqrt(var);
    	DecimalFormat df = new DecimalFormat("#.##");
    	String formatted = df.format(std_d); 
    	System.out.println("Standard Deviation for Salary="+formatted);
      //System.out.println("JDBC Exit from class");

   }
   
   catch (Exception e)
   {
     System.err.println("Could not load JDBC driver");
     System.out.println("Exception: " + e);
     e.printStackTrace();
   }

   
 }
}

