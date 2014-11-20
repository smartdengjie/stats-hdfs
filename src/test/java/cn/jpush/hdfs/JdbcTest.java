package cn.jpush.hdfs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

public class JdbcTest {

    public static void main(String[] args) throws SQLException {
        String src = args[0];
        String path = args[1];
        String table = "test.active";

        String create =
                new StringBuilder("execute:CREATE TABLE IF NOT EXISTS ").append(table).append(" (")
                        .append("appkey STRING,").append("platform STRING,").append("uid BIGINT,")
                        .append("open INT,").append("useTime INT,").append("time BIGINT")
                        .append(")").append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'")
                        .toString();

        String loadLocal = "execute:LOAD DATA LOCAL INPATH '" + path + "' INTO TABLE " + table;
        String loadHdfs = "execute:LOAD DATA INPATH '" + path + "' INTO TABLE " + table;
        String queryUser =
                "query:SELECT appkey,platform,count(DISTINCT uid) FROM " + table
                        + " GROUP BY appkey,platform";
        String queryOpen =
                "query:SELECT appkey,platform,sum(open) FROM " + table
                        + " GROUP BY appkey,platform";

        HiveExec user = new HiveExec();
        user.addCmd(create);
        if (src.equals("local")) {
            user.addCmd(loadLocal);
        } else if (src.equals("hdfs")) {
            user.addCmd(loadHdfs);
        }
        user.addCmd(queryUser);
        new Thread(user).start();

        HiveExec open = new HiveExec();
        open.addCmd(queryOpen);
        new Thread(open).start();
    }
}


class HiveExec implements Runnable {
    public static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private ArrayList<String> commandList = new ArrayList<String>();

    HiveExec() {

    }

    HiveExec(String cmd) {
        commandList.add(cmd);
    }

    public void addCmd(String cmd) {
        commandList.add(cmd);
    }

    public void run() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            Connection con =
                    DriverManager.getConnection("jdbc:hive2://192.168.250.242:10000/default",
                            "wangxb", "");
            Statement stmt = con.createStatement();

            for (String command : commandList) {
                System.out.println(new Date() + ": Running " + command);
                String[] cmd = command.split(":");
                command = cmd[1];
                if (cmd[0].equals("execute")) {
                    boolean success = stmt.execute(command);
                    if (!success) {
                        System.err.println("fail to execute " + command);
                    }
                } else {
                    ResultSet res = stmt.executeQuery(command);
                    while (res.next()) {
                        System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t"
                                + res.getString(3));
                    }
                }
                System.out.println(new Date() + ": Finish " + command);
            }

            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
