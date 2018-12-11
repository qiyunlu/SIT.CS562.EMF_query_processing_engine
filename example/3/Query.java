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
        mfs.put("cust", new ArrayList<String>());
        mfs.put("month", new ArrayList<Integer>());
        mfs.put("sum_0_quant", new ArrayList<Integer>());
        mfs.put("count_0_quant", new ArrayList<Integer>());
        mfs.put("sum_1_quant", new ArrayList<Integer>());
        mfs.put("count_1_quant", new ArrayList<Integer>());
        mfs.put("sum_2_quant", new ArrayList<Integer>());
        mfs.put("count_2_quant", new ArrayList<Integer>());

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
                String cust = rs.getString("cust");
                String month = rs.getString("month");

                boolean _flag = true;
                for(int i = 0; i < mfs.get("cust").size(); i++) {
                    if(cust.equals((String)mfs.get("cust").get(i)+"")&&month.equals((int)mfs.get("month").get(i)+"")) {
                        _flag = false;
                        break;
                    }
                }
                // add a new combination of grouping attributes
                if(_flag) {
                    mfs.get("cust").add(cust+"");
                    mfs.get("month").add(Integer.parseInt(month+""));
                    mfs.get("sum_0_quant").add(null);
                    mfs.get("count_0_quant").add(null);
                    mfs.get("sum_1_quant").add(null);
                    mfs.get("count_1_quant").add(null);
                    mfs.get("sum_2_quant").add(null);
                    mfs.get("count_2_quant").add(null);
                }
            }


            // aggregate functions for grouping variable 0
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

                for(int i = 0; i < mfs.get("cust").size(); i++) {
                    // Sigma(0)
                    if((!(mfs.get("cust").get(i)+"").equals("null"))&&(!(mfs.get("month").get(i)+"").equals("null"))&&(cust.equals((String)mfs.get("cust").get(i)+""))&&(month.equals((int)mfs.get("month").get(i)+""))) {
                        // sum_0_quant
                        String _sum_0_quant = mfs.get("sum_0_quant").get(i)+"";
                        if(_sum_0_quant.equals("null")) {
                            _sum_0_quant = "0";
                        }
                        mfs.get("sum_0_quant").set(i, (int)(Double.parseDouble(_sum_0_quant)+Double.parseDouble(quant)));
                        // count_0_quant
                        String _count_0_quant = mfs.get("count_0_quant").get(i)+"";
                        if(_count_0_quant.equals("null")) {
                            _count_0_quant = "0";
                        }
                        mfs.get("count_0_quant").set(i, (int)(Double.parseDouble(_count_0_quant)+1d));
                    }
                }
            }

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

                for(int i = 0; i < mfs.get("cust").size(); i++) {
                    // Sigma(1)
                    if((!(mfs.get("cust").get(i)+"").equals("null"))&&(!(mfs.get("month").get(i)+"").equals("null"))&&(cust.equals((String)mfs.get("cust").get(i)+""))&&(Double.parseDouble(month)<Double.parseDouble((int)mfs.get("month").get(i)+""))) {
                        // sum_1_quant
                        String _sum_1_quant = mfs.get("sum_1_quant").get(i)+"";
                        if(_sum_1_quant.equals("null")) {
                            _sum_1_quant = "0";
                        }
                        mfs.get("sum_1_quant").set(i, (int)(Double.parseDouble(_sum_1_quant)+Double.parseDouble(quant)));
                        // count_1_quant
                        String _count_1_quant = mfs.get("count_1_quant").get(i)+"";
                        if(_count_1_quant.equals("null")) {
                            _count_1_quant = "0";
                        }
                        mfs.get("count_1_quant").set(i, (int)(Double.parseDouble(_count_1_quant)+1d));
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

                for(int i = 0; i < mfs.get("cust").size(); i++) {
                    // Sigma(2)
                    if((!(mfs.get("cust").get(i)+"").equals("null"))&&(!(mfs.get("month").get(i)+"").equals("null"))&&(cust.equals((String)mfs.get("cust").get(i)+""))&&(Double.parseDouble(month)>Double.parseDouble((int)mfs.get("month").get(i)+""))) {
                        // sum_2_quant
                        String _sum_2_quant = mfs.get("sum_2_quant").get(i)+"";
                        if(_sum_2_quant.equals("null")) {
                            _sum_2_quant = "0";
                        }
                        mfs.get("sum_2_quant").set(i, (int)(Double.parseDouble(_sum_2_quant)+Double.parseDouble(quant)));
                        // count_2_quant
                        String _count_2_quant = mfs.get("count_2_quant").get(i)+"";
                        if(_count_2_quant.equals("null")) {
                            _count_2_quant = "0";
                        }
                        mfs.get("count_2_quant").set(i, (int)(Double.parseDouble(_count_2_quant)+1d));
                    }
                }
            }


            // print output
            int rowNum = 0;
            System.out.println("cust                       month     avg_1_quant     avg_0_quant     avg_2_quant");
            System.out.println("================================================================================");
            for(int i = 0; i < mfs.get("cust").size(); i++) {
                // having clause
                if(true) {
                    rowNum++;
                    // cust
                    if((mfs.get("cust").get(i)+"").equals("null")) {
                        System.out.print(String.format("%-16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%-16s", (String)mfs.get("cust").get(i)));
                    }
                    // month
                    if((mfs.get("month").get(i)+"").equals("null")) {
                        System.out.print(String.format("%16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%16d", (int)mfs.get("month").get(i)));
                    }
                    // avg_1_quant
                    if(((mfs.get("sum_1_quant").get(i)+"").equals("null"))||((mfs.get("count_1_quant").get(i)+"").equals("null"))) {
                        System.out.print(String.format("%16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%16.3f", Double.parseDouble((int)mfs.get("sum_1_quant").get(i)+"")/Double.parseDouble((int)mfs.get("count_1_quant").get(i)+"")));
                    }
                    // avg_0_quant
                    if(((mfs.get("sum_0_quant").get(i)+"").equals("null"))||((mfs.get("count_0_quant").get(i)+"").equals("null"))) {
                        System.out.print(String.format("%16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%16.3f", Double.parseDouble((int)mfs.get("sum_0_quant").get(i)+"")/Double.parseDouble((int)mfs.get("count_0_quant").get(i)+"")));
                    }
                    // avg_2_quant
                    if(((mfs.get("sum_2_quant").get(i)+"").equals("null"))||((mfs.get("count_2_quant").get(i)+"").equals("null"))) {
                        System.out.print(String.format("%16s", "null"));
                    }
                    else {
                        System.out.print(String.format("%16.3f", Double.parseDouble((int)mfs.get("sum_2_quant").get(i)+"")/Double.parseDouble((int)mfs.get("count_2_quant").get(i)+"")));
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
