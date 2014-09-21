package gpo.dbhelper;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class MySQLHelper {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static boolean debug = true;
	
	private String dbName;
	private String dbServer;
	private String dbUser;
	private String dbPassword;
	private boolean stop = false;
	
	private Connection connection	= null;
	private Statement statement		= null;
	
	private void openConnection() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		connection = (Connection) DriverManager.getConnection("jdbc:mysql://"+dbServer+"/"+dbName, dbUser, dbPassword);
	}
	
	public void closeConnection() {
		if (connection != null)
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	/**
	 * @description Method execute sql and return true if no execeptions.
	 * @param sql
	 * @return
	 * Petro Gordiievych
	 * 21.09.2014
	 */
	public boolean executeUpdate(String sql) {
		boolean flag = true;
		if (stop) {
			this.closeConnection();
			return false;
		}
		if (sql != null && sql.length() > 0) {
			try {
				if (connection == null || connection.isClosed()) {
					openConnection();
				}
				if (statement == null) {
					statement = (Statement) connection.createStatement();
				}
				_debug("Updated/Inserted: "+statement.executeUpdate(sql));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				flag = false;
				stop = true;
			} catch (SQLException e) {
				e.printStackTrace();
				flag = false;
				stop = true;
			}
			if (!flag)
				_debug(sql);
		}
		return flag;
	}
	
	public MySQLHelper(String dbName, String dbServer, String dbUser,
			String dbPassword) {
		super();
		this.dbName = dbName;
		this.dbServer = dbServer;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
	}
	
	private void _debug(String str) {
		if (debug)
			System.out.println(str);
	}
}
