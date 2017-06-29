package data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.ibatis.common.jdbc.ScriptRunner;

public class RunSqlScript extends MainClass {

  public static void runSqlScript() throws ClassNotFoundException, SQLException {

    String sqlScriptFilePath = processed_dir + sqlinserts_file;

    Class.forName(web_dbdriver);
    Connection con = DriverManager.getConnection(web_dburl, web_dbusr, web_dbpwd);

    try {

      ScriptRunner sr = new ScriptRunner(con, false, false);
      Reader reader = new BufferedReader(new FileReader(sqlScriptFilePath));
      sr.runScript(reader);

    } catch (Exception e) {
      System.err.println("Failed to Execute" + sqlScriptFilePath + " The error is "
          + e.getMessage());
    }
  }
}
