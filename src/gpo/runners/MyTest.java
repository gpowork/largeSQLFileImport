package gpo.runners;

import gpo.dbhelper.MySQLHelper;
import gpo.filereader.GFileReader;

public class MyTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName = "e:\\toServer\\dump20092014.sql";
		GFileReader.debug = true;
		GFileReader gR = null;
		try {
			gR = new GFileReader(fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		MySQLHelper mH = new MySQLHelper("fortest", "localhost", "root", "");
		boolean flag = false;
		try {
			if (gR != null) {
				while(gR.stillHas()) {
					String str = gR.getSQLStatement();
					if (str != null && str.length() > 0) {
						if (str.indexOf("CREATE TABLE `zr`") > -1) {
							System.out.println("found zr:");
							flag = true;
						}
						if (flag)
							mH.executeUpdate(
									str
									);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			mH.closeConnection();
		}
	}

}
