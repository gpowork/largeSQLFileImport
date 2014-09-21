package gpo.filereader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/*
 * This class reads file and return distinct statements which can be run in MySQL.
 */
public class GFileReader {
	
	public static boolean debug = false;
	
	private File file;
	private boolean stillHas = false;
	private BufferedReader br = null;
	private long statementsCounter	= 0;
	private long byteCounter		= 0;
	private long fileSize			= 0;
	
	private void _debug(String str) {
		if (debug)
			System.out.println(str);
	}
	
	/**
	 * @param fileName
	 * @throws Exception 
	 */
	public GFileReader(String fileName) throws Exception {
		_debug("Got file name: "+fileName);
		file = new File(fileName);
		if (!file.exists())
			throw new FileNotFoundException("File doesn't exit");
		fileSize = file.length();
		if (fileSize == 0)
			throw new Exception("File is empty");
		_debug("File exist. Size: "+file.length()/1e6+" MB.");
		stillHas = true;
	}
	
	/**
	 * @description Method return distinct SQL statement.
	 * @return String
	 * Petro Gordiievych
	 * 21.09.2014
	 */
	public String getSQLStatement() {
		if (file != null) {
			if (br == null) {
				try {
//					br = new BufferedReader(new FileReader(file));
					br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-16"));
					_debug("BufferedReader is ready.");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (br != null) {
				StringBuilder str = new StringBuilder();
				int i = 0;
				int watchDog = 1000;
				boolean multiline = false;
				try {
					while(br.ready() && i < watchDog && (multiline || i == 0)) {
						// Check if this line is not comment:
						String _tS = br.readLine();
						if (_tS != null) {
							byteCounter += _tS.getBytes().length*2;
							_tS = _tS.trim();
							if (!multiline) {
								if (_tS.indexOf("CREATE TABLE") == 0 && _tS.indexOf("ENGINE=") < 0) {
									multiline = true;
								_debug("Found multiline tag. "+_tS);
								}
							} else {
								if (_tS.indexOf("ENGINE=") > -1) {
									multiline = false;
//								_debug("Found end multiline tag.");
								}
							}
							if (_tS != null && _tS.length() > 0 && (_tS.indexOf("-") > 4 || _tS.indexOf("-") < 0) && (_tS.indexOf("/") > 1 || _tS.indexOf("/") < 0)) {
								str.append(_tS);
								i++;
							}
						}
					}
//					_debug("Finish reading.");
					if (!br.ready())
						closeBuffer();
				} catch (IOException e) {
					e.printStackTrace();
				}
				statementsCounter++;
//				_debug(str.toString());
				_debug("Processed: "+byteCounter+", "+ Math.ceil(byteCounter * 1.0 / fileSize * 100) +"%");
				return str.toString();
			}
		}
//		_debug("Result is NULL.");
		return null;
	}
	
	private void closeBuffer() {
		stillHas = false;
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		_debug("Buffer closed. Found "+ statementsCounter +" statements.");
	}

	/**
	 * @description Method show is buffer still has lines to read.
	 * Petro Gordiievych
	 * 21.09.2014
	 */
	public boolean stillHas() {
		return stillHas;
	}
	
}
