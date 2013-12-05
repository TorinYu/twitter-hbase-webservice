package com.myeclipseide.ws;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.sun.jersey.spi.resource.Singleton;

import java.sql.*;

import com.mysql.jdbc.Driver;

@Produces("text/hml")
@Path("/")
@Singleton
public class CustomersResource {

  //private TreeMap<Integer, Customer> customerMap = new TreeMap<Integer, Customer>();

  public CustomersResource() throws ClassNotFoundException, SQLException {
    // hardcode a single customer into the database for demonstration
	Class.forName("com.mysql.jdbc.Driver");
	conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter?user=root&password=82267185");
	//stmt = conn.createStatement();
  }
  

  @GET
  public String getCustomers() {
    //List<Customer> customers = new ArrayList<Customer>();
    //customers.addAll(customerMap.values());
	String test = "Please indicate your query type in the url!";
    return test;
  }

  @GET
  @Path("q1")
  public String heartbeatRqst() {
	Date dNow = new Date();
	SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
	String msg = "MatrixHacker, 9531-9820-5769 9946-3830-3272 2339-4174-2685\n" + ft.format(dNow) + "\n";
    return msg;
  }
  
  @GET
  @Path("q2")
  public String Rqst2(@QueryParam("time") String reqstTime) throws ParseException, ClassNotFoundException, SQLException {
	SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
	Date temp_date = ft.parse(reqstTime);
	SimpleDateFormat ft2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss '+0000' yyyy");
	String toDataBase = ft2.format(temp_date);
	
	//Class.forName("com.mysql.jdbc.Driver");
	//Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter?user=root&password=82267185");
	Statement stmt=conn.createStatement();
	ResultSet rSet = stmt.executeQuery("select tidtext from tweetsq2 where create_at = \"" + toDataBase + "\"");
	
	String res = "";
	while (rSet.next()) {
		res = rSet.getString(1);
    }

	String[] results = res.split("\t");
	
	StringBuffer sbuf = new StringBuffer();
	sbuf.append("MatrixHacker, 9531-9820-5769 9946-3830-3272 2339-4174-2685\n");
	for(String oneline : results) {
		sbuf.append(oneline + "\n");
	}
    return sbuf.toString();
  }
  
  @GET
  @Path("q3")
  public String Rqst3(@QueryParam("userid_min") String minId, @QueryParam("userid_max") String maxId) throws ClassNotFoundException, SQLException {
	String msg = "MatrixHacker, 9531-9820-5769 9946-3830-3272 2339-4174-2685\n";
	//String msg = "The requested user IDs are from \n" + minId + "to " + maxId;
	
	//Class.forName("com.mysql.jdbc.Driver");
	//Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter?user=root&password=82267185");
	Statement stmt=conn.createStatement();
	ResultSet rSet = stmt.executeQuery("select SUM(count) from tweetsq3 where userid >= " + minId + " AND userid <= " + maxId);
	
	String res = "";
	while (rSet.next()) {
		res = rSet.getString(1);
    }
	
    return (msg+res+"\n");
  }
  
  @GET
  @Path("q4")
  public String Rqst4(@QueryParam("userid") String userId) throws ClassNotFoundException, SQLException {
	//String msg = "MatrixHacker, 9531-9820-5769 9946-3830-3272 2339-4174-2685\n";
	//String msg = "The requested user ID is \n" + userId;
	//select userid from 'tweetsq4' where o_userid = (queryvalue);
	
	Statement stmt=conn.createStatement();
	ResultSet rSet = stmt.executeQuery("select userid from tweetsq4 where o_userid = " + userId);
	StringBuffer sbuf = new StringBuffer();
	sbuf.append("MatrixHacker, 9531-9820-5769 9946-3830-3272 2339-4174-2685\n");
/**	
	ResultSetMetaData meta = rSet.getMetaData();
	 for (int index = 1; index <= meta.getColumnCount(); index++)
	 {
	    System.out.println("Column " + index + " is named " + meta.getColumnName(index));
	 }
**/	
	String res = "";
	while (rSet.next())
    {
		res = rSet.getString(1);
		//System.out.println(res);
    }
	String[] results = res.split(",");
	//System.out.print(sbuf.toString());
	
	

	for(String oneline : results) {
		sbuf.append(oneline + "\n");
	}
    return sbuf.toString();
  }
  
  private Connection conn;
  
}