package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM student"));
            System.out.println(cleaner.executeUpdate("DELETE FROM teacher"));
            //hdd = student; sdd = teacher
            PreparedStatement hddSt = conn.prepareStatement(
                    "INSERT INTO hdd (name, capacity, inch, rpm, connface, buffer) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");
            PreparedStatement ssdSt = conn.prepareStatement(
                    "INSERT INTO ssd (name, capacity, inch, controller, memtype, readSpeed, writeSpeed) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (line[0].equals("HDD")) {
                    hddSt.setString(1, line[1]);
                    hddSt.setInt(2, Integer.parseInt(line[2]));
                    hddSt.setString(3, line[3]);
                    hddSt.setString(4, line[4]);
                    hddSt.setString(5, line[5]);
                    hddSt.setInt(6, Integer.parseInt(line[6]));

                    hddSt.addBatch();
                } else {
                    ssdSt.setString(1, line[1]);
                    ssdSt.setInt(2, Integer.parseInt(line[2]));
                    ssdSt.setString(3, line[3]);
                    ssdSt.setString(4, line[4]);
                    ssdSt.setString(5, line[5]);
                    ssdSt.setInt(6, Integer.parseInt(line[6]));
                    ssdSt.setInt(7, Integer.parseInt(line[7]));
                    ssdSt.addBatch();
                }
            }
            int[] hddRes = hddSt.executeBatch();
            int[] ssdRes = hddSt.executeBatch();
            for (int num: hddRes) {
                System.out.println(num);
            }

            for (int num: ssdRes) {
                System.out.println(num);
            }
            cleaner.close();
            hddSt.close();
            ssdSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM hdd WHERE capacity > 500");
            while (rs.next()) {
                System.out.print(rs.getString("name"));
                System.out.print(" ");
                System.out.print(rs.getString("inch"));
                System.out.print(" ");
                System.out.println(rs.getString("capacity"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE ssd SET readSpeed='80386' WHERE inch='M.2'");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM ssd");
                    while (rs.next()) {
                        System.out.println(rs.getString("readSpeed"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM ssd");
                    while (rs.next()) {
                        System.out.println(rs.getString("readSpeed"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
