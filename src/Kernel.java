/*
This project is to build a query processing engine for Ad-Hoc OLAP queries.
The query construct is based on an extended SQL syntax known as MF and EMF queries

This class is the entry to the whole project.
The class is logically divided into 2 parts:
    1. First. Read Phi operands and database information from files. Translate SQL language to Java language.
    2. Second. Use these translated values and the extended SQL syntax to generate a file "Query.java", which can run independently and get the same output as Ad-Hoc OLAP query.
*/

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Kernel {
    
    // Phi operands
    // SELECT ATTRIBUTE(S)
    public static List<String> SA = new ArrayList<String>();
    // NUMBER OF GROUPING VARIABLES(n)
    public static int NGV = 0;
    // GROUPING ATTRIBUTES(V)
    public static List<String> GA = new ArrayList<String>();
    // F-VECT([F])
    public static List<List> FV = new ArrayList<List>();
    // SELECT CONDITION-VECT([Sigma])
    public static List<String> SCV = new ArrayList<String>();
    // HAVING CONDITION(G)
    public static String HC = null;
    
    // the user name, password and database url of postgreSQL
    public static String usr = null;
    public static String pwd = null;
    public static String url = null;
    // table's name and structure
    public static String tableName = null;
    public static List<String> tableStruct = new ArrayList<String>();
    
    // table attributes' and aggregates' type information
    public static Map<String, String> TAT = new HashMap<>();
    
    
	public static void main(String[] args) {
		
	    // read database info from file "database"
        List<String> infos = ReadFile.readByLine("database");
        for(int i = 0; i < infos.size(); i++) {
            if(infos.get(i).indexOf("database user") != -1) {
                usr = infos.get(i+1);
            }
            else if(infos.get(i).indexOf("database password") != -1) {
                pwd = infos.get(i+1);
            }
            else if(infos.get(i).indexOf("database url") != -1) {
                url = infos.get(i+1);
            }
            else if(infos.get(i).indexOf("table name") != -1) {
                tableName = infos.get(i+1);
            }
            else { /* do nothing */ }
        }
        
	    // load database class, and initialize it
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Database: Loading successful.");
        }
        catch(Exception e) {
            System.out.println("Database: Loading failed.");
            e.printStackTrace();
        }
        
        try {
            Connection conn = DriverManager.getConnection(url, usr, pwd);
            System.out.println("Database: Connecting successful.\n");

            Statement stat = conn.createStatement();

            // read table attributes' info
            ResultSet rs = stat.executeQuery("SELECT * FROM information_schema.columns WHERE table_name = '"+tableName+"'");
            
            while (rs.next()) {
                
                String column_name = rs.getString("column_name");
                String data_type = rs.getString("data_type");
                
                // store column
                tableStruct.add(column_name);
                // specify data type
                String specifiedT = ManipulateString.sqlTToJavaT(data_type);
                // store transfered <name, type> pair
                TAT.put(column_name, specifiedT);
            }
        }
        catch(SQLException e) {
            System.out.println("Database: Connection URL or username or password error!");
            e.printStackTrace();
        }
	    
        
		// read operands from file "input"
	    List<String> lines = ReadFile.readByLine("input");
		
	    // store each operands
        // SELECT ATTRIBUTE
	    for(int i = 0; i < lines.size(); i++) {
            if(lines.get(i).indexOf("SELECT ATTRIBUTE") != -1) {
                String[] str = lines.get(i+1).replaceAll(" ", "").split(",");
                for(String s : str) {
                    SA.add(s);
                }
            }
            // NUMBER OF GROUPING VARIABLES
            else if(lines.get(i).indexOf("NUMBER OF GROUPING VARIABLES") != -1) {
                NGV = Integer.parseInt(lines.get(i+1));
            }
            // GROUPING ATTRIBUTES
            else if(lines.get(i).indexOf("GROUPING ATTRIBUTES") != -1) {
                String[] str = lines.get(i+1).replaceAll(" ", "").split(",");
                for(String s : str) {
                    GA.add(s);
                }
            }
            // F-VECT
            else if(lines.get(i).indexOf("F-VECT") != -1) {
                String[] str = lines.get(i+1).replaceAll(" ", "").split(";");
                for(String S : str) {
                    List<String> list = new ArrayList<String>();
                    if((S.equals(""))) {
                        list.add(S);
                        FV.add(list);
                    }
                    else {
                        String[] str2 = S.split(",");
                        for(String s : str2) {
                            // specify aggregate's type and store in TAT (and FV)
                            String _type = null;
                            String[] _str = s.split("_");
                            // avg = sum/count
                            if(_str[0].equals("avg")) {
                                // sum
                                String _sum = "sum_"+_str[1]+"_"+_str[2];
                                _type = ManipulateString.specifyAggregateType(_sum);
                                TAT.put(_sum, _type);
                                list.add(_sum);
                                // count
                                String _count = "count_"+_str[1]+"_"+_str[2];
                                TAT.put(_count, "int");
                                list.add(_count);
                            }
                            else {
                                list.add(s);
                            }
                            _type = ManipulateString.specifyAggregateType(s);
                            TAT.put(s, _type);
                        }
                        // remove duplicates
                        List<String> list2 = new ArrayList<String>();
                        for (String dup : list) {
                            if (Collections.frequency(list2, dup) < 1) {
                                list2.add(dup);
                            }
                        }
                        FV.add(list2);
                    }
                }
            }
            // SELECT CONDITION-VECT
            else if(lines.get(i).indexOf("SELECT CONDITION-VECT") != -1) {
                String[] str = lines.get(i+1).split(";");
                for(int j = 0; j < str.length; j++) {
                    SCV.add(ManipulateString.sqlSToJavaC(str[j], j));
                }
            }
            // HAVING CONDITION
            else if(lines.get(i).indexOf("HAVING CONDITION") != -1) {
                HC = ManipulateString.sqlHToJavaC(lines.get(i+1));
            }
            else { /* do nothing */ }
        }
		
	    /* check variables
	    System.out.println(SA);
	    System.out.println(NGV);
	    System.out.println(GA);
	    System.out.println(FV);
	    System.out.println(SCV);
	    System.out.println(HC);
	    System.out.println(tableStruct);
	    System.out.println(TAT);
	    //*/
	    
	    // start constructing java program
	    FileWriter writer = null;
		try {
		    // create a java file
		    String className = "Query";
		    File file = new File(className+".java");
		    if(file.createNewFile()) {
		        System.out.println("File \""+className+".java\" created successfully.");
            }
		    else {
		        System.out.println("File already exists.");
                // do nothing but finish program
		    }
		    
		    // add code to file
		    writer = new FileWriter(className+".java", true);
		    
		    writer.write("import java.sql.*;"+"\n");
		    writer.write("import java.util.*;"+"\n");
		    writer.write(""+"\n");
		    writer.write("public class "+className+" {"+"\n");
		    writer.write(""+"\n");
            writer.write("    // mf-structure"+"\n");
            writer.write("    public static Map<String, List> mfs = new HashMap<>();"+"\n");
            writer.write("    // result set"+"\n");
            writer.write("    public static ResultSet rs = null;"+"\n");
            writer.write(""+"\n");
            writer.write(""+"\n");
            writer.write("    public static void main(String[] args) {"+"\n");
            writer.write(""+"\n");
            writer.write("        // the user name, password and database url of postgreSQL"+"\n");
            writer.write("        String usr =\""+usr+"\";"+"\n");
            writer.write("        String pwd =\""+pwd+"\";"+"\n");
            writer.write("        String url =\""+url+"\";"+"\n");
            writer.write(""+"\n");
            writer.write("        // build mf-structure"+"\n");
            for(int i = 0; i < GA.size(); i++) {
                String type = TAT.get(GA.get(i));
                if(type.equals("int")) { type = "Integer"; }
                writer.write("        mfs.put(\""+GA.get(i)+"\", new ArrayList<"+type+">());"+"\n");
            }
            for(int i = 0; i < FV.size(); i++) {
                if(FV.get(i).get(0).equals("")) {
                    continue;
                }
                for(int j = 0; j < FV.get(i).size(); j++) {
                    String type = TAT.get(FV.get(i).get(j));
                    if(type.equals("int")) { type = "Integer"; }
                    writer.write("        mfs.put(\""+FV.get(i).get(j)+"\", new ArrayList<"+type+">());"+"\n");
                }
            }
            writer.write(""+"\n");
            writer.write("        // load database class, and initialize it"+"\n");
            writer.write("        try {"+"\n");
            writer.write("            Class.forName(\"org.postgresql.Driver\");"+"\n");
            writer.write("            System.out.println(\"Database: Loading successful.\");"+"\n");
            writer.write("        }"+"\n");
            writer.write("        catch(Exception e) {"+"\n");
            writer.write("            System.out.println(\"Database: Loading failed.\");"+"\n");
            writer.write("            e.printStackTrace();"+"\n");
            writer.write("        }"+"\n");
            writer.write(""+"\n");
            writer.write("        try {"+"\n");
            writer.write("            Connection conn = DriverManager.getConnection(url, usr, pwd);"+"\n");
            writer.write("            System.out.println(\"Database: Connecting successful.\\n\");"+"\n");
            writer.write(""+"\n");
            writer.write("            Statement stat = conn.createStatement();"+"\n");
            writer.write(""+"\n");
            writer.write("            // first scan to insert all combinations of grouping attributes"+"\n");
            writer.write("            rs = stat.executeQuery(\"SELECT * FROM sales\");"+"\n");
            writer.write("            while (rs.next()) {"+"\n");
            writer.write(""+"\n");
            writer.write("                // get grouping attributes from a record"+"\n");
            for(int i = 0; i < GA.size(); i++) {
                String ga = GA.get(i);
                writer.write("                String "+ga+" = rs.getString(\""+ga+"\");"+"\n");
            }
            writer.write(""+"\n");
            writer.write("                boolean _flag = true;"+"\n");
            writer.write("                for(int i = 0; i < mfs.get(\""+GA.get(0)+"\").size(); i++) {"+"\n");
            String ifF0 = ""+GA.get(0)+".equals(("+TAT.get(GA.get(0))+")mfs.get(\""+GA.get(0)+"\").get(i)+\"\")";
            for(int i = 1; i < GA.size(); i++) {
                ifF0 += "&&"+GA.get(i)+".equals(("+TAT.get(GA.get(i))+")mfs.get(\""+GA.get(i)+"\").get(i)+\"\")";
            }
            writer.write("                    if("+ifF0+") {"+"\n");
            writer.write("                        _flag = false;"+"\n");
            writer.write("                        break;"+"\n");
            writer.write("                    }"+"\n");
            writer.write("                }"+"\n");
            writer.write("                // add a new combination of grouping attributes"+"\n");
            writer.write("                if(_flag) {"+"\n");
            for(int i = 0; i < GA.size(); i++) {
                String type = TAT.get(GA.get(i));
                if(type.equals("int")) {
                    writer.write("                    mfs.get(\""+GA.get(i)+"\").add(Integer.parseInt("+GA.get(i)+"+\"\"));"+"\n");
                }
                else if(type.equals("Double")) {
                    writer.write("                    mfs.get(\""+GA.get(i)+"\").add(Double.parseDouble("+GA.get(i)+"+\"\"));"+"\n");
                }
                else {
                    writer.write("                    mfs.get(\""+GA.get(i)+"\").add("+GA.get(i)+"+\"\");"+"\n");
                }
            }
            for(int i = 0; i < FV.size(); i++) {
                if(FV.get(i).get(0).equals("")) {
                    continue;
                }
                for(int j = 0; j < FV.get(i).size(); j++) {
                    writer.write("                    mfs.get(\""+FV.get(i).get(j)+"\").add(null);"+"\n");
                }
            }
            writer.write("                }"+"\n");
            writer.write("            }"+"\n");
            writer.write(""+"\n");
            for(int GVNum = 0; GVNum <= NGV; GVNum++) {
                writer.write(""+"\n");
                if(FV.get(GVNum).get(0).equals("")) {
                    writer.write("            // no aggregate function for grouping variable "+GVNum+"\n");
                    continue;
                }
                writer.write("            // aggregate functions for grouping variable "+GVNum+"\n");
                writer.write("            rs = stat.executeQuery(\"SELECT * FROM sales\");"+"\n");
                writer.write("            while (rs.next()) {"+"\n");
                writer.write(""+"\n");
                writer.write("                // get all attributes from a record"+"\n");
                for(int i = 0; i < tableStruct.size(); i++) {
                    String attr = tableStruct.get(i);
                    writer.write("                String "+attr+" = rs.getString(\""+attr+"\");"+"\n");
                }
                writer.write(""+"\n");
                writer.write("                for(int i = 0; i < mfs.get(\""+GA.get(0)+"\").size(); i++) {"+"\n");
                writer.write("                    // Sigma("+GVNum+")"+"\n");
                writer.write("                    if("+SCV.get(GVNum)+") {"+"\n");
                for(int i = 0; i < FV.get(GVNum).size(); i++) {
                    String af = (String)FV.get(GVNum).get(i);
                    String[] _af = af.split("_");
                    writer.write("                        // "+af+"\n");
                    writer.write("                        String _"+af+" = mfs.get(\""+af+"\").get(i)+\"\";"+"\n");
                    writer.write("                        if(_"+af+".equals(\"null\")) {"+"\n");
                    if(_af[0].equals("sum")||_af[0].equals("count")) {
                        writer.write("                            _"+af+" = \"0\";"+"\n");
                    }
                    else {
                        writer.write("                            _"+af+" = "+_af[2]+";"+"\n");
                    }
                    writer.write("                        }"+"\n");
                    if(_af[0].equals("sum")) {
                        writer.write("                        mfs.get(\""+af+"\").set(i, ("+TAT.get(af)+")(Double.parseDouble(_"+af+")+Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                    else if(_af[0].equals("count")) {
                        writer.write("                        mfs.get(\""+af+"\").set(i, ("+TAT.get(af)+")(Double.parseDouble(_"+af+")+1d));"+"\n");
                    }
                    else if(_af[0].equals("max")) {
                        writer.write("                        mfs.get(\""+af+"\").set(i, ("+TAT.get(af)+")Math.max(Double.parseDouble(_"+af+"), Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                    else {
                        writer.write("                        mfs.get(\""+af+"\").set(i, ("+TAT.get(af)+")Math.min(Double.parseDouble(_"+af+"), Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                }
                writer.write("                    }"+"\n");
                writer.write("                }"+"\n");
                writer.write("            }"+"\n");
            }
            writer.write(""+"\n");
            writer.write(""+"\n");
            writer.write("            // print output"+"\n");
            writer.write("            int rowNum = 0;"+"\n");
            String attrLine = "";
            for(int i = 0; i < SA.size(); i++) {
                String attr = SA.get(i);
                String type = TAT.get(attr);
                if(type.equals("String")) {
                    attrLine += String.format("%-16s", attr);
                }
                else {
                    attrLine += String.format("%16s", attr);
                }
            }
            writer.write("            System.out.println(\""+attrLine+"\");"+"\n");
            writer.write("            System.out.println(\"");
            for(int i = 0; i < SA.size(); i++) {
                writer.write("================");
            }
            writer.write("\");"+"\n");
            writer.write("            for(int i = 0; i < mfs.get(\""+GA.get(0)+"\").size(); i++) {"+"\n");
            writer.write("                // having clause"+"\n");
            writer.write("                if("+HC+") {"+"\n");
            writer.write("                    rowNum++;"+"\n");
            for(int i = 0; i < SA.size(); i++) {
                String attr = SA.get(i);
                String[] _attr = attr.split("_");
                String type = TAT.get(attr);
                writer.write("                    // "+attr+"\n");
                if(_attr[0].equals("avg")&&_attr.length == 3) {
                    String _sum = "sum_"+_attr[1]+"_"+_attr[2];
                    String _count = "count_"+_attr[1]+"_"+_attr[2];
                    writer.write("                    if(((mfs.get(\""+_sum+"\").get(i)+\"\").equals(\"null\"))||((mfs.get(\""+_count+"\").get(i)+\"\").equals(\"null\"))) {"+"\n");
                    writer.write("                        System.out.print(String.format(\"%16s\", \"null\"));"+"\n");
                    writer.write("                    }"+"\n");
                    writer.write("                    else {"+"\n");
                    writer.write("                        System.out.print(String.format(\"%16.3f\", Double.parseDouble(("+TAT.get(_sum)+")mfs.get(\""+_sum+"\").get(i)+\"\")/Double.parseDouble(("+TAT.get(_count)+")mfs.get(\""+_count+"\").get(i)+\"\")));"+"\n");
                    writer.write("                    }"+"\n");
                }
                else {
                    if(type.equals("Double")) {
                        writer.write("                    if((mfs.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%16s\", \"null\"));"+"\n");
                        writer.write("                    }"+"\n");
                        writer.write("                    else {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%16.3f\", ("+type+")mfs.get(\""+attr+"\").get(i)));"+"\n");
                        writer.write("                    }"+"\n");
                    }
                    else if(type.equals("int")) {
                        writer.write("                    if((mfs.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%16s\", \"null\"));"+"\n");
                        writer.write("                    }"+"\n");
                        writer.write("                    else {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%16d\", ("+type+")mfs.get(\""+attr+"\").get(i)));"+"\n");
                        writer.write("                    }"+"\n");
                    }
                    else {
                        writer.write("                    if((mfs.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%-16s\", \"null\"));"+"\n");
                        writer.write("                    }"+"\n");
                        writer.write("                    else {"+"\n");
                        writer.write("                        System.out.print(String.format(\"%-16s\", ("+type+")mfs.get(\""+attr+"\").get(i)));"+"\n");
                        writer.write("                    }"+"\n");
                    }
                }
            }
            writer.write("                    System.out.println(\"\");"+"\n");
            writer.write("                }"+"\n");
            writer.write("            }"+"\n");
            writer.write(""+"\n");
            writer.write("            System.out.println(\"\\nSuccessfully run. \"+rowNum+\" row(s) affected.\\n\");"+"\n");
            writer.write(""+"\n");
            writer.write("        }"+"\n");
            writer.write("        catch(SQLException e) {"+"\n");
            writer.write("            System.out.println(\"Database: URL or username or password or table name error!\");"+"\n");
            writer.write("            e.printStackTrace();"+"\n");
            writer.write("        }"+"\n");
            writer.write("    }"+"\n");
		    writer.write("}"+"\n");
		    
		}
		catch(Exception e) {
		    e.printStackTrace();
		}
		finally {
		    if(writer != null) {
		        try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
		    }
		}
	}

}
