package data;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

public class ReadPropertyValues extends MainClass {

  public static void getPropValues() throws IOException {

    String log4jConfPath = System.getProperty("user.dir") + "/log4j.properties";
    PropertyConfigurator.configure(log4jConfPath);

    Properties prop = new Properties();
    InputStream input = null;

    try {
      input = new FileInputStream("./application.properties");
    } catch (IOException ex) {
      input = new FileInputStream("config.properties");
    } finally {
      prop.load(input);

      testmode = Boolean.valueOf(prop.getProperty("testmode"));
      test_file = prop.getProperty("testfile");

      geonamesdebugmode = Boolean.valueOf(prop.getProperty("geonamesdebugmode"));
      fieldtypesdebugmode = Boolean.valueOf(prop.getProperty("fieldtypesdebugmode"));

      nrowchecks = Integer.valueOf(prop.getProperty("nrowchecks"));
      pvalue_nrowchecks = Double.valueOf(prop.getProperty("pvalue_nrowchecks"));


      executeSQLInserts = Boolean.valueOf(prop.getProperty("executesqlinserts"));

      geonames_dbdriver = prop.getProperty("geonames_dbdriver");
      geonames_dburl = prop.getProperty("geonames_dburl");
      geonames_dbusr = prop.getProperty("geonames_dbusr");
      geonames_dbpwd = prop.getProperty("geonames_dbpwd");

      web_dbdriver = prop.getProperty("web_dbdriver");
      web_dburl = prop.getProperty("web_dburl");
      web_dbusr = prop.getProperty("web_dbusr");
      web_dbpwd = prop.getProperty("web_dbpwd");

      sqlinserts_file = prop.getProperty("sqlinserts_file");
      csvfiles_dir = prop.getProperty("csvfiles_dir");
      tmp_dir = prop.getProperty("tmp_dir");
      processed_dir = prop.getProperty("processed_dir");
      newformat_dir = prop.getProperty("newformat_dir");
      enriched_dir = prop.getProperty("enriched_dir");
      missinggeoreference_dir = prop.getProperty("missinggeoreference_dir");


      country_code = prop.getProperty("countrycode");

      shapes_file = prop.getProperty("shapes_file");

      st1postcode = prop.getProperty("st1postcode");
      st2postcode = prop.getProperty("st2postcode");
      st1city = prop.getProperty("st1city");
      st2city = prop.getProperty("st2city");
      st3city = prop.getProperty("st3city");

      imageRegex = prop.getProperty("imageRegex");
      phoneRegex = prop.getProperty("phoneRegex");
      cityRegex = prop.getProperty("cityRegex");
      archiveRegex = prop.getProperty("archiveRegex");
      documentRegex = prop.getProperty("documentRegex");
      openinghoursRegex = prop.getProperty("openinghoursRegex");
      dateRegex = prop.getProperty("dateRegex");
      yearRegex = prop.getProperty("yearRegex");
      currencyRegex = prop.getProperty("currencyRegex");
      percentageRegex = prop.getProperty("percentageRegex");
      postcodeRegex = prop.getProperty("postcodeRegex");
      nutsRegex = prop.getProperty("nutsRegex");
      latitudeRegex = prop.getProperty("latitudeRegex");
      longitudeRegex = prop.getProperty("longitudeRegex");
      latlngRegex = prop.getProperty("latlngRegex");
      possiblenameRegex = prop.getProperty("possiblenameRegex");

      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
