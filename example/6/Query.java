import java.sql.*;
import java.util.*;

public class Query {

    // mf-structure
    public static Map<String, List> mfs = new HashMap<>();
    // result set
    public static ResultSet rs = null;


    public static void main(String[] args) {

        // the user name, password and database url of postgreSQL
        String usr ="postgres";
        String pwd ="26892681147";
        String url ="jdbc:postgresql://localhost:5432/postgres";

        // build mf-structure
        mfs.put("prod", new ArrayList<String>());
        mfs.put("quant", new ArrayList<Integer>());
        mfs.put("count_1_prod", new ArrayList<Integer>());
        mfs.put("count_2_prod", new ArrayList<Integer>());

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

            // first scan to insert all combinations of grouping attributes
            rs = stat.executeQuery("SELECT * FROM sales");
            while (rs.next()) {

                // get grouping attributes from a record
                String prod = rs.getString("prod");
                String quant = rs.getString("quant");

                boolean _flag = true;
                for(int i = 0; i < mfs.get("prod").size(); i++) {
                    if(prod.equals((String)mfs.get("prod").get(i)+"")&&quant.equals((int)mfs.get("quant").get(i)+"")) {
                        _flag = false;
                        break;
                    }
                }
                // add a new combination of grouping attributes
                if(_flag) {
                    mfs.get("prod").add(prod+"");
                    mfs.get("quant").add(Integer.parseInt(quant+""));
                    mfs.get("count_1_prod").add(null);
                    mfs.get("count_2_prod").add(null);
                }
            }


            // no aggregate function for grouping variable 0

            // aggregate functions for grouping variable 1
            rs = stat.executeQuery("SELECT * FROM sales");
            while (rs.next()) {

                // get all attributes from a record
                String cust = rs.getString("cust");
                String prod = rs.getString("prod");
                String day = rs.getString("day");
                String month = rs.getString("month");
                String year = rs.getString("year");
                String state = rs.getString("state");
                String quant = rs.getString("quant");

                for(int i = 0; i < mfs.get("prod").size(); i++) {
                    // Sigma(1)
                    if((!(mfs.get("prod").get(i)+"").equals("null"))&&(prod.equals((String)mfs.get("prod").get(i)+""))) {
                        // count_1_prod
                        String _count_1_prod = mfs.get("count_1_prod").get(i)+"";
                        if(_count_1_prod.equals("null")) {
                            _count_1_prod = "0";
                        }
                        mfs.get("count_1_prod").set(i, (int)(Double.parseDouble(_count_1_prod)+1d));
                    }
                }
            }

            // aggregate functions for grouping variable 2
            rs = stat.executeQuery("SELECT * FROM sales");
            while (rs.next()) {

                // get all attributes from a record
                String cust = rs.getString("cust");
                String prod = rs.getString("prod");
                String day = rs.getString("day");
                String month = rs.getString("month");
                String year = rs.getString("year");
                String state = rs.getString("state");
                String quant = rs.getString("quant");

                for(int i = 0; i < mfs.get("prod").size(); i++) {
                    // Sigma(2)
                    if((!(mfs.get("prod").get(i)+"").equals("null"))&&(!(mfs.get("quant").get(i)+"").equals("null"))&&(prod.equals((String)mfs.get("prod").get(i)+""))&&(Double.parseDouble(quant)<Double.parseDouble((int)mfs.get("quant").get(i)+""))) {
                        // count_2_prod
                        String _count_2_prod = mfs.get("count_2_prod").get(i)+"";
                        if(_count_2_prod.equals("null")) {
                            _count_2_prod = "0";
                        }
                        mfs.get("count_2_prod").set(i, (int)(Double.parseDouble(_count_2_prod)+1d));
                    }
                }
            }


            // print output
            int rowNum = 0;
            System.out.println("prod                       quant");
            System.out.println("================================");
            for(int i = 0; i < mfs.get("prod").size(); i++) {
                // having clause
                if((!(mfs.get("count_2_prod").get(i)+"").equals("null"))&&(!(mfs.get("count_1_prod").get(i)+"").equals("null"))&&(((int)mfs.get("count_2_prod").get(i)+"").equals((int)mfs.get("count_1_prod").get(i)/2+""))) {
                    rowNum++;
                    // prod
                    if((mfs.get("prod").get(i)+"").equals("null")) {
                        System.out.print(String.format("%-16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%-16s", (String)mfs.get("prod").get(i)));
                    }
                    // quant
                    if((mfs.get("quant").get(i)+"").equals("null")) {
                        System.out.print(String.format("%16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%16d", (int)mfs.get("quant").get(i)));
                    }
                    System.out.println("");
                }
            }

            System.out.println("\nSuccessfully run. "+rowNum+" row(s) affected.\n");

        }
        catch(SQLException e) {
            System.out.println("Database: URL or username or password or table name error!");
            e.printStackTrace();
        }
    }
}
