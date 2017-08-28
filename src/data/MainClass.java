package data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.EmailValidator;
import org.apache.commons.validator.routines.UrlValidator;
import org.geonames.InvalidParameterException;
import org.geotools.filter.text.cql2.CQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;


public class MainClass {

  private static Connection conn = null;

  private final static Logger log = LoggerFactory.getLogger(MainClass.class);

  protected static boolean testmode = false;
  protected static String test_file = "test.csv";

  public static boolean geonamesdebugmode = false;
  protected static boolean fieldtypesdebugmode = false;
  
  public static boolean removeExistingBData = false;
  public static boolean executeSQLqueries = false;

  protected static int nrowchecks = 20;
  protected static double pvalue_nrowchecks = 0.5;

  protected static int nchecks = 21;
  protected static int ncolchecks = 0;
  protected static String resul[] = null;

  // Delimiters used in CSV files
  protected static String DELIMITER = ";";
  protected static final String DELIMITER2 = ",";
  protected static final String NEW_LINE = "\n";



  protected static String country_code = "AT";

  protected static String shapes_file = "/NUTS_2013_SHP/data/NUTS_RG_01M_2013.shp";

  // Directories where files are processed
  protected static String csvfiles_dir = "file: config.properties";

  protected static String tmp_dir = "file: config.properties";
  protected static String processed_dir = "file: config.properties";
  protected static String newformat_dir = "file: config.properties";
  protected static String enriched_dir = "file: config.properties";
  protected static String missinggeoreference_dir = "file: config.properties";
  protected static String sqlinserts_file = "file: config.properties";


  protected static String geonames_dbdriver = "org.postgresql.Driver";
  protected static String geonames_dburl = "jdbc:postgresql://127.0.0.1:5432/geonames";
  protected static String geonames_dbusr = "postgres";
  protected static String geonames_dbpwd = "postgres";

  protected static String web_dbdriver = "org.postgresql.Driver";
  protected static String web_dburl = "jdbc:postgresql://127.0.0.1:5432/cvienna";
  protected static String web_dbusr = "postgres";
  protected static String web_dbpwd = "postgres";

  protected static String st1postcode =
      "select name,admin3name,code,latitude,longitude from postalcodes where code like ? order by code asc";
  protected static String st2postcode =
      "select name,admin3name,code,latitude,longitude from postalcodes where code = ? order by code asc";
  protected static String st1city =
      "select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname = ? order by population desc";
  protected static String st2city =
      "select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname like ? or asciiname like ? order by population desc";
  protected static String st3city =
      "select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname like ? or asciiname like ? order by population desc";

  protected static String imageRegex = ".*.(jpg|gif|png|bmp|ico)$";
  protected static String phoneRegex = "^\\+?[0-9. ()-]{10,25}$";
  protected static String archiveRegex = ".*.(zip|7z|bzip(2)?|gzip|jar|t(ar|gz)|dmg)$";
  protected static String documentRegex =
      ".*.(doc(x|m)?|pp(t|s|tx)|o(dp|tp)|pub|pdf|csv|xls(x|m)?|r(tf|pt)|info|txt|tex|x(ml|html|ps)|rdf(a|s)?|owl)$";
  protected static String openinghoursRegex =
      "([a-z ]+ )?(mo(n(day)?)?|tu(e(s(day)?)?)?|we(d(nesday)?)?|th(u(r(s(day)‌​?)?)?)?|fr(i(day)?)?‌​|sa(t(urday)?)?|su(n‌​(day)?)?)(-|:| ).*|([a-z ]+ )?(mo(n(tag)?)?|di(e(n(stag)?)?)?|mi(t(woch)?)?|do(n(er(s(tag)‌​?)?)?)?|fr(i(tag)?)?‌​|sa(m(stag)?)?|do(n(erstag)?)?)(-|:| ).*";
  protected static String dateRegex =
      "([0-9]{2})?[0-9]{2}( |-|\\/|.)[0-3]?[0-9]( |-|\\/|.)([0-9]{2})?[0-9]{2}";
  protected static String yearRegex = "^(?:18|20)\\d{2}$";
  protected static String currencyRegex =
      "^(\\d+|\\d+[.,']\\d+)\\p{Sc}|\\p{Sc}(\\d+|\\d+[.,']\\d+)$";
  protected static String percentageRegex = "^(\\d+|\\d+[.,']\\d+)%|%(\\d+|\\d+[.,']\\d+)$";
  protected static String postcodeRegex = "^[0-9]{2}$|^[0-9]{4}$";
  protected static String nutsRegex = "\\w{3,5}";
  protected static String shapeRegex =
      "point\\s*\\(([+-]?\\d+\\.?\\d+)\\s*,?\\s*([+-]?\\d+\\.?\\d+)\\)";
  protected static String latitudeRegex = "/^-?([1-8]?[1-9]|[1-9]0)\\.{1}\\d{4,9}$/";
  protected static String longitudeRegex =
      "^-?([1]?[1-7][1-9]|[1]?[1-8][0]|[1-9]?[0-9])\\.{1}\\d{4,9}$";
  protected static String latlngRegex = "([+-]?\\d+\\.?\\d+)\\s*,\\s*([+-]?\\d+\\.?\\d+)";
  protected static String possiblenameRegex = ".*[0-9]+.*";
  protected static String cityRegex = ".*[a-z]{3,30}.*";

  // csv file name
  private static String name = "";

  private static String header[] = null;
  
  private static String columnTypes[][] = null;
  private static int sum[][] = null;

  private static String[] tmp_cities = null;
  private static String[] tmp_possiblenames = null;
  private static boolean[] tmp_postcol = null;

  private static String line;
  private static int comma = 0;
  private static int dotcomma = 0;

  public static void setFormatLatLng(LocationModel obj, String field) {
    String newFormat = "";
    boolean startCopying = false;
    boolean isLatitudeRead = false;

    for (int i = 0; i < field.length(); ++i) {
      char x = field.charAt(i);
      if (x == '(') {
        startCopying = !startCopying;
      }
      if (x != '(' && startCopying && !isLatitudeRead) {
        newFormat += x;
      }
      if (startCopying && x == ' ') {
        obj.setLongitude(new BigDecimal(new Double(newFormat), new MathContext(
            newFormat.length() - 2)));
        newFormat = "";
        isLatitudeRead = true;
      }
      if (x != ' ' && x != ')' && isLatitudeRead) {
        newFormat += x;
      }
      if (x == ')') {
        obj.setLatitude(new BigDecimal(new Double(newFormat), new MathContext(
            newFormat.length() - 2)));
        startCopying = !startCopying;
      }
    }
  }

  public static void findFieldTypes(String dir, String name) throws NumberFormatException,
      CQLException, IOException, NumberParseException, SQLException {

    br = new BufferedReader(new InputStreamReader(new FileInputStream(dir + name), "iso-8859-1"));

    double confidence_limit = (nrowchecks * (100 - pvalue_nrowchecks * 100)) / 100;

    int i = 0;
    comma = 0;
    dotcomma = 0;
    while ((line = br.readLine()) != null) {


      if (i == 0) {
        log.info("\n--------------------------------------------------------------------------------------------------------");

        for (int j = 0; j < line.length(); j++) {
          if (line.charAt(j) == ',')
            comma++;
          if (line.charAt(j) == ';')
            dotcomma++;
        }
        // log.info("Num comma: " + comma + " and num of dotcomma: " + dotcomma);
        if (dotcomma > comma) {
          DELIMITER = ";";
          // log.info("DELIMITER IS DOT COMMA!!");
        } else {
          DELIMITER = ",";
          // log.info("DELIMITER IS COMMA!!");
        }

        // we first detect the first header row
        if (line != null) {

          // split by delimiter with unlimited tokens having empty space
          ncolchecks = line.split("\\" + DELIMITER, -1).length;
          // if (testmode || fieldtypesdebugmode || geonamesdebugmode) {

          log.info("file: " + name);
          log.info("columns detected: " + ncolchecks);
          log.info("--delimiter detected: \"" + DELIMITER + "\"");
          
          
          
          header = new String[ncolchecks];
          String[]  value = new String[ncolchecks];
          
          value = line.split(DELIMITER);
          
          for (int j = 0; j < value.length; j++) {
              header[j] = value[j].substring(0, 1).toUpperCase() + value[j].substring(1).toLowerCase();
          }
          log.info("header detected: "+java.util.Arrays.toString(header));
          
          
          log.info("processing pls wait...");

          // exception for some files with a not defined header
          if (ncolchecks < 10)
            ncolchecks = 10;


          // }

          columnTypes = new String[nrowchecks][ncolchecks];

          // for (int r = 0; r < ncolchecks; r++) {
          // for (int s = 0; s < ncolchecks; s++) {
          // columnTypes[r][s] = null;
          // }
          // }
          sum = new int[nchecks][ncolchecks];

          tmp_cities = new String[ncolchecks];
          tmp_possiblenames = new String[ncolchecks];
          tmp_postcol = new boolean[ncolchecks];
          resul = new String[ncolchecks*2]; //we duplicate the number of columns just to be shure we don't get a segmentation fault
        }
        
        // exception for some headers (which are not in the first row) , where we will remove "," inside quotes for csv files with "," delimiter
      } else if (i < nrowchecks) {

        // log.info("line: " + i + " -------------------------------------------------");
        // log.info("before.." + line);
        if (DELIMITER.contentEquals(",")) {
          boolean inQuotes = false;

          String str = line;
          String copy = new String();
          for (int k = 0; k < str.length(); ++k) {
            if (str.charAt(k) == '"')
              inQuotes = !inQuotes;
            if (str.charAt(k) == ',' && inQuotes)
              copy += ' ';
            else
              copy += str.charAt(k);
          }

          line = copy;

        } else {
          line = line.replaceAll(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ",");
        }

        String[] value = line.split(DELIMITER);



        // ParameterizedSparqlString qs =
        // new ParameterizedSparqlString(""
        // + "prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#>\n" + "\n"
        // + "select ?resource where {\n" + "  ?resource rdfs:label ?label\n" + "}");

        int j = 0;
        while (j < value.length) {


          // Literal val = ResourceFactory.createLangLiteral(value[j], "en");
          // qs.setParam("val1", val);

          // System.out.println(qs);
          // String val = value[j].replaceAll("", "").toLowerCase();

          String val = value[j].toLowerCase().replaceAll("\"", "").trim();

          // log.info("check: " + val);



          if (val.matches(phoneRegex)) {
            PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
            // try {
            PhoneNumber phone = phoneUtil.parse(val, "AT");
            if (phoneUtil.isValidNumber(phone)) {
              columnTypes[i][j] = "phone";
            }
            // } catch (NumberParseException e) {
            // // System.err.println("NumberParseException was thrown: " + e.toString());
            // }
          } else if (EmailValidator.getInstance().isValid(val)) {
            // log.info("we have an email ! in fil " + i + " and col " + j);
            columnTypes[i][j] = "email";
          } else if (UrlValidator.getInstance().isValid(val)
              || UrlValidator.getInstance().isValid("http://" + val)) {
            if (val.matches(imageRegex)) {
              columnTypes[i][j] = "image";
            } else if (val.matches(archiveRegex)) {
              columnTypes[i][j] = "archive";
            } else if (val.matches(documentRegex)) {
              columnTypes[i][j] = "document";
            } else {
              columnTypes[i][j] = "url";
            }
          } else if (val.matches(openinghoursRegex)) {
            columnTypes[i][j] = "openinghours";
          } else if (val.matches(dateRegex)) {
            columnTypes[i][j] = "date";
          } else if (val.matches(yearRegex)) {
            columnTypes[i][j] = "year";
          } else if (val.replaceAll(" ", "").matches(currencyRegex)) {
            columnTypes[i][j] = "currency";
          } else if (val.replaceAll(" ", "").matches(percentageRegex)) {
            columnTypes[i][j] = "percentage";
          } else if (val.matches(postcodeRegex)) {
            rs = GeonamesSQLQueries.getPostcodeLatLng(Integer.valueOf(val), conn);
            if (rs != null) {
              // log.info("WE HAVE A POSTCODE!!! with name " + rs.getString(2) + " for[" + val
              // + "] , latitude is " + rs.getString(3) + " and longitude is " + rs.getString(4)
              // + " in line " + i);
              columnTypes[i][j] = "postcode";
            }
          } else if (val.matches(nutsRegex)) {
            // log.info("value is  " + val);
            double[] latlng = ReadGISShapes.getNutsLatLng(val.toUpperCase());
            if (latlng != null) {
              // log.info("result is  " + latlng[0]);
              switch (val.length()) {
                case 3:
                  columnTypes[i][j] = "nuts1";
                  break;
                case 4:
                  columnTypes[i][j] = "nuts2";
                  break;
                case 5:
                  columnTypes[i][j] = "nuts3";
                  break;
              }
              // log.info("WE HAVE NUTS!!! with latitude is " + latlng[0] + " and longitude is "
              // + latlng[1] + " in line " + i);
            }

          } else if (val.matches(shapeRegex)) {
            columnTypes[i][j] = "shape";
          } else if (val.matches(latitudeRegex)) {
            columnTypes[i][j] = "latitude";
          } else if (val.matches(longitudeRegex)) {
            columnTypes[i][j] = "longitude";
          } else if (val.matches(latlngRegex)) {
            columnTypes[i][j] = "latlong";
          } else if (val.matches(cityRegex)) {

            rs = GeonamesSQLQueries.getCityLatLng(val, conn);
            if (rs != null) {
              if (tmp_cities[j] == null) {
                columnTypes[i][j] = "city";
                tmp_cities[j] = val;
                // log.info("WE HAVE A CITY!!! with name " + rs.getString(2) + " for[" + val
                // + "] , latitude is " + rs.getString(3) + " and longitude is " + rs.getString(4)
                // + " in line " + i);
              } else if (!tmp_cities[j].contentEquals(val)) {
                // log.info("for line " + j + " tmp_cities[j] is " + tmp_cities[j] + " with val "
                // + val);
                columnTypes[i][j] = "city";
                tmp_cities[j] = val;
                // log.info("WE HAVE A CITY!!! with name " + rs.getString(2) + " for[" + val
                // + "] , latitude is " + rs.getString(3) + " and longitude is " + rs.getString(4)
                // + " in line " + i);
              }

            } else if (!val.matches(possiblenameRegex)) {

              // we check that possible names are not repeated
              if (tmp_possiblenames[j] == null) {
                columnTypes[i][j] = "possiblename";
                tmp_possiblenames[j] = val;
              } else if (!tmp_possiblenames[j].contentEquals(val)) {
                // log.info("for line " + j + " tmp_possiblenames[j] is " + tmp_possiblenames[j]
                // + " with val " + val);
                columnTypes[i][j] = "possiblename";
                tmp_possiblenames[j] = val;
              }
            }

            // ParameterizedSparqlString qs =
            // new ParameterizedSparqlString(
            // "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX foaf: <http://xmlns.com/foaf/0.1/> select distinct ?C ?Long ?Lat where {?X geo:lat ?Lat ; geo:long ?Long; a ?C . { { ?X foaf:name \""
            // + val + "\"@en. }  } } LIMIT 100 ");
            //
            //
            // QueryExecution exec =
            // QueryExecutionFactory
            // .sparqlService("http://dbpedia.org/sparql", qs.asQuery());
            //
            //
            //
            // ResultSet results = exec.execSelect();
            //
            // // while (results.hasNext()) {
            // // // As RobV pointed out, don't use the `?` in the variable
            // // // name here. Use *just* the name of the variable.
            // // System.out.println(results.next().get("resource"));
            // // }
            //
            // ResultSetFormatter.out(results);
          }
          j++;
        }

        for (int k = 1; k < nrowchecks; k++) {
          for (int l = 0; l < ncolchecks; l++) {
            // log.info("[" + k + "][" + l + "]:" + columnTypes[k][l] + "; ");
          }
        }

        for (int m = 0; m < nchecks; m++) {
          for (int n = 0; n < ncolchecks; n++) {
            sum[m][n] = 0;
          }
        }

        for (int k = 1; k < nrowchecks; k++) {
          int nmails = 0;
          int nurls = 0;
          int nphones = 0;
          int ncities = 0;
          int npostcodes = 0;
          int nophours = 0;
          int ndates = 0;
          int nyears = 0;
          int nimages = 0;
          int narchives = 0;
          int ndocuments = 0;
          int ncurrencies = 0;
          int npercentages = 0;
          int nshapes = 0;
          int nlatitudes = 0;
          int nlongitudes = 0;
          int nlatlong = 0;
          int nnuts1 = 0;
          int nnuts2 = 0;
          int nnuts3 = 0;
          int npossiblenames = 0;

          for (int l = 0; l < ncolchecks; l++) {

            if (columnTypes[k][l] != null) {
              switch (columnTypes[k][l]) {
                case "email":
                  nmails++;
                  sum[0][l] += 1;
                  break;
                case "url":
                  nurls++;
                  sum[1][l] += 1;
                  break;
                case "phone":
                  nphones++;
                  sum[2][l] += 1;
                  break;
                case "city":
                  ncities++;
                  sum[3][l] += 1;
                  break;
                case "postcode":
                  int trueCount = 0;
                  for (int t = 0; t < tmp_postcol.length; t++) {
                    if (tmp_postcol[t] == true)
                      trueCount++;
                    if (trueCount == 3)
                      break;
                  }
                  // avoid columns which have matched more than 3 postcodes
                  if (trueCount != 3) {
                    npostcodes++;
                    sum[4][l] += 1;
                    tmp_postcol[l] = true;
                  }
                  break;
                case "openinghours":
                  nophours++;
                  sum[5][l] += 1;
                  break;
                case "date":
                  ndates++;
                  sum[6][l] += 1;
                  break;
                case "year":
                  nyears++;
                  sum[7][l] += 1;
                  break;
                case "image":
                  nimages++;
                  sum[8][l] += 1;
                  break;
                case "archive":
                  narchives++;
                  sum[9][l] += 1;
                  break;
                case "document":
                  ndocuments++;
                  sum[10][l] += 1;
                  break;
                case "currency":
                  ncurrencies++;
                  sum[11][l] += 1;
                  break;
                case "percentage":
                  npercentages++;
                  sum[12][l] += 1;
                  break;
                case "shape":
                  nshapes++;
                  sum[13][l] += 1;
                  break;
                case "latitude":
                  nlatitudes++;
                  sum[14][l] += 1;
                  break;
                case "longitude":
                  nlongitudes++;
                  sum[15][l] += 1;
                  break;
                case "latlong":
                  nlatlong++;
                  sum[16][l] += 1;
                  break;
                case "nuts1":
                  nnuts1++;
                  sum[17][l] += 1;
                  break;
                case "nuts2":
                  nnuts2++;
                  sum[18][l] += 1;
                  break;
                case "nuts3":
                  nnuts3++;
                  sum[19][l] += 1;
                  break;
                case "possiblename":
                  npossiblenames++;
                  sum[20][l] += 1;
                  break;
              }
            }
          }
          if (fieldtypesdebugmode) {
            log.info("\nchecking > " + line);
            log.info("row[" + k + "]: " + nmails + " emails, " + nurls + " urls, " + nphones
                + " phones, " + ncities + " cities, " + npostcodes + " postcodes, " + nophours
                + " opening hours, " + ndates + " dates, " + nyears + " years, " + nimages
                + " images, " + narchives + " archives, " + ndocuments + " documents, "
                + ncurrencies + " currencies, " + npercentages + " percentages, " + nshapes
                + " shapes, " + nlatitudes + " latitudes, " + nlongitudes + " longitudes, "
                + nlatlong + " latlong, " + nnuts1 + " nuts1, " + nnuts2 + " nuts2 " + nnuts3
                + " nuts3, " + npossiblenames + " possible names");
            log.info("sum:");
            for (int m = 0; m < nchecks; m++) {
              for (int n = 0; n < ncolchecks; n++) {
                if (sum[m][n] != 0) {

                  switch (m) {
                    case 0:
                      log.info(sum[m][n] + " emails in col " + n);
                      break;
                    case 1:
                      log.info(sum[m][n] + " urls in col " + n);
                      break;
                    case 2:
                      log.info(sum[m][n] + " phones in col " + n);
                      break;
                    case 3:
                      log.info(sum[m][n] + " cities in col " + n);
                      break;
                    case 4:
                      log.info(sum[m][n] + " postcodes in col " + n);
                      break;
                    case 5:
                      log.info(sum[m][n] + " opening hours in col " + n);
                      break;
                    case 6:
                      log.info(sum[m][n] + " dates in col " + n);
                      break;
                    case 7:
                      log.info(sum[m][n] + " years in col " + n);
                      break;
                    case 8:
                      log.info(sum[m][n] + " images in col " + n);
                      break;
                    case 9:
                      log.info(sum[m][n] + " archives in col " + n);
                      break;
                    case 10:
                      log.info(sum[m][n] + " documents in col " + n);
                      break;
                    case 11:
                      log.info(sum[m][n] + " currencies in col " + n);
                      break;
                    case 12:
                      log.info(sum[m][n] + " percentages in col " + n);
                      break;
                    case 13:
                      log.info(sum[m][n] + " shapes in col " + n);
                      break;
                    case 14:
                      log.info(sum[m][n] + " latitudes in col " + n);
                      break;
                    case 15:
                      log.info(sum[m][n] + " longitudes in col " + n);
                      break;
                    case 16:
                      log.info(sum[m][n] + " latlong in col " + n);
                      break;
                    case 17:
                      log.info(sum[m][n] + " nuts1 in col " + n);
                      break;
                    case 18:
                      log.info(sum[m][n] + " nuts2 in col " + n);
                      break;
                    case 19:
                      log.info(sum[m][n] + " nuts3 in col " + n);
                      break;
                    case 20:
                      log.info(sum[m][n] + " possible names in col " + n);
                      break;
                  }

                }
              }
            }
          }
        }

      } else if (i == nrowchecks) {
        if (testmode || fieldtypesdebugmode || geonamesdebugmode)
          log.info("\n\nFINAL RESULT:");
        for (int m = 0; m < nchecks; m++) {
          for (int n = 0; n < ncolchecks; n++) {
            if (sum[m][n] >= confidence_limit) {

              switch (m) {
                case 0:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " emails in col " + n);
                  resul[n] = "email";
                  break;
                case 1:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " urls in col " + n);
                  resul[n] = "url";
                  break;
                case 2:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " phones in col " + n);
                  resul[n] = "phone";
                  break;
                case 3:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " cities in col " + n);
                  resul[n] = "city";
                  break;
                case 4:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " postcodes in col " + n);
                  resul[n] = "postcode";
                  break;
                case 5:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " opening hours in col " + n);
                  resul[n] = "openinghours";
                  break;
                case 6:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " dates in col " + n);
                  resul[n] = "date";
                  break;
                case 7:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " years in col " + n);
                  resul[n] = "year";
                  break;
                case 8:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " images in col " + n);
                  resul[n] = "image";
                  break;
                case 9:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " archives in col " + n);
                  resul[n] = "archive";
                  break;
                case 10:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " documents in col " + n);
                  resul[n] = "document";
                  break;
                case 11:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " currencies in col " + n);
                  resul[n] = "currency";
                  break;
                case 12:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " percentages in col " + n);
                  resul[n] = "percentage";
                  break;
                case 13:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " shapes in col " + n);
                  resul[n] = "shape";
                  break;
                case 14:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " latitudes in col " + n);
                  resul[n] = "latitude";
                  break;
                case 15:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " longitudes in col " + n);
                  resul[n] = "longitude";
                  break;
                case 16:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " latlong in col " + n);
                  resul[n] = "latlong";
                  break;
                case 17:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " nuts1 in col " + n);
                  resul[n] = "nuts1";
                  break;
                case 18:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " nuts2 in col " + n);
                  resul[n] = "nuts2";
                  break;
                case 19:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " nuts3 in col " + n);
                  resul[n] = "nuts2";
                  break;
                case 20:
                  if (testmode || fieldtypesdebugmode || geonamesdebugmode)
                    log.info(sum[m][n] + " possible names in col " + n);
                  resul[n] = "possiblename";
                  break;
              }
            }
          }
        }
        break;
      }
      i++;
    }
  }


  public static void processFile(File source, File dest) throws IOException, ParseException,
      CQLException, InterruptedException, InvalidParameterException, SQLException {

    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(source), "iso-8859-1"));

    ArrayList<LocationModel> new_format = new ArrayList<LocationModel>();


    Pattern patternCsvName = Pattern.compile("([A-Z].*|[a-zA-Z0-9]{2,20}).csv");
    Matcher matcher = patternCsvName.matcher(name);
    String csvname_match = null;
    if (matcher.find()) {
      // log.info("CSV Type: " + matcher.group());
      csvname_match = matcher.group().split(".csv")[0];
    }

    String line;

    // i is the row position
    int i = 0;

    while ((line = br.readLine()) != null) {


      if (DELIMITER.contentEquals(",")) {
        boolean inQuotes = false;

        String str = line;
        String copy = new String();
        for (int k = 0; k < str.length(); ++k) {
          if (str.charAt(k) == '"')
            inQuotes = !inQuotes;
          if (str.charAt(k) == ',' && inQuotes)
            copy += ' ';
          else
            copy += str.charAt(k);
        }

        line = copy;
        // System.out.println(line);

      } else {
        line = line.replaceAll(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ",");
      }

      // we split the row by ";" separator
      String[] value = line.split(DELIMITER);

      // for each row we create an instance of the Model
      LocationModel loc = new LocationModel();


      // j is the column position
      int j = 0;


      // log.info("avlue..." + value.length);
      while (j < value.length) {

        // type is a regex of 20 chars or any capital letter start from the CSV name
        if (csvname_match != null) {
          loc.setType(csvname_match);

        }

        loc.setCsvName(name);

        //log.info("index is "+j+ " with size "+resul.length);
        if (resul[j] != null) {
          switch (resul[j]) {
            case "email":
              loc.setEmail(value[j]);
              break;
            case "url":
              loc.setWebsite(value[j]);
              break;
            case "phone":
              loc.setPhone(value[j]);
              break;
            case "city":
              loc.setCity(value[j]);

              boolean haslatlng = false;
              for (int p = 0; p < nrowchecks; p++) {
                for (int q = 0; q < ncolchecks; q++) {
                  if (columnTypes[p][q] == "latitude" || columnTypes[p][q] == "latlong"
                      || columnTypes[p][q] == "shape") {
                    haslatlng = true;
                    break;
                  }
                }
              }
              if (!haslatlng) {
                rs = GeonamesSQLQueries.getCityLatLng(value[j], conn);
                if (rs != null) {
                  // if (tmp_cities[j] != null) {
                  GeonamesSQLQueries.setGeonameResult(loc, rs);
                  // }
                }
              }
              break;
            case "postcode":
              loc.setPostcode(value[j]);

              haslatlng = false;
              for (int p = 0; p < nrowchecks; p++) {
                for (int q = 0; q < ncolchecks; q++) {
                  if (columnTypes[p][q] == "latitude" || columnTypes[p][q] == "latlong"
                      || columnTypes[p][q] == "shape") {
                    haslatlng = true;
                    break;
                  }
                }
              }
              if (!haslatlng && value[j].matches(postcodeRegex)) {
                rs = GeonamesSQLQueries.getPostcodeLatLng(Integer.valueOf(value[j]), conn);
                if (rs != null) {
                  // if (tmp_cities[j] != null) {
                  GeonamesSQLQueries.setGeonameResult(loc, rs);
                  // }
                }
              }
              break;
            case "openinghours":
              loc.setSchedule(value[j]);
              break;
            case "date":
              loc.setDate(value[j]);
              break;
            case "year":
              // loc.setYear(value[j]);
              break;
            case "image":
              loc.setImage(value[j]);
              break;
            case "archive":
              // loc.setArchive(value[j]);
              break;
            case "document":
              // loc.setDocument(value[j]);
              break;
            case "currency":
              // loc.setCurrency(value[j]);
              break;
            case "percentage":
              // loc.setPercentage(value[j]);
              break;
            case "shape":
              setFormatLatLng(loc, value[j]);
              break;
            case "latitude":
              if (NumberUtils.isNumber(value[j].replace(",", "."))) {
                loc.setLatitude(new BigDecimal(value[j].replace(",", ".")));
              } else {
                log.info("NOT NUMBERIC, latitude is " + value[j]);
              }
              break;
            case "longitude":
              if (NumberUtils.isNumber(value[j].replace(",", "."))) {
                loc.setLongitude(new BigDecimal(value[j].replace(",", ".")));
              } else {
                log.info("NOT NUMBERIC, longitude is " + value[j]);
              }
              break;
            case "latlong":
              setFormatLatLng(loc, value[j]);
              break;
            case "nuts1":
              double[] latlng = ReadGISShapes.getNutsLatLng(value[j]);
              if (latlng != null) {
                loc.setLatitude(new BigDecimal(latlng[0]));
                loc.setLongitude(new BigDecimal(latlng[1]));
              }
              break;
            case "nuts2":
              latlng = ReadGISShapes.getNutsLatLng(value[j]);
              if (latlng != null) {
                loc.setLatitude(new BigDecimal(latlng[0]));
                loc.setLongitude(new BigDecimal(latlng[1]));
              }
              break;
            case "nuts3":
              latlng = ReadGISShapes.getNutsLatLng(value[j]);
              if (latlng != null) {
                loc.setLatitude(new BigDecimal(latlng[0]));
                loc.setLongitude(new BigDecimal(latlng[1]));
              }
              break;
            case "possiblename":
              loc.setName(value[j]);
              break;
          }
        } else {
        	if (header[j] != null) {
                if (loc.getOther() == null) {
                    loc.setOther(header[j]+": " + value[j]);
                  } else {
                    loc.setOther(loc.getOther() + " ## "+header[j]+": " + value[j]);
                  }
        	} else {
          if (loc.getOther() == null) {
            loc.setOther("no_match[" + j + "]: " + value[j]);
          } else {
            loc.setOther(loc.getOther() + " ## no_match[" + j + "]: " + value[j]);
          }
        }
        }

        j++;
      }


      if (i != 0) {
        if (loc.getName() == null) {
          loc.setName(csvname_match);
        }

        new_format.add(loc);
      }

      i++;
    }
    br.close();

    if (dest.getAbsolutePath().contains(tmp_dir)) {
      GenerateCSVFiles.CreateEnrichedCSV(source, dest, new_format);
    }

    GenerateCSVFiles.CreateFormattedCSV(dest, new_format);

  }

  static int no_insert_bestfiles = 0;
  static int no_insert_nutsfiles = 0;
  static int no_insert_possiblefiles = 0;

  private static java.sql.ResultSet rs;

  private static BufferedReader br;

private static EntityManagerFactory entityManagerFactory;



  public static void createDir(String dir) throws IOException {
    File folder = new File(dir);
    if (folder.exists()) {
      FileUtils.forceDelete(folder);
      folder.mkdir();
    } else {
      folder.mkdir();
    }
  }

  public static void main(String[] args) throws IOException, ParseException, CQLException,
      InterruptedException, InvalidParameterException, SQLException, ExecutionException,
      NumberParseException, ClassNotFoundException {

    ReadPropertyValues.getPropValues();
    
    ApplicationContext context = new ClassPathXmlApplicationContext(
            "classpath*:**/applicationContext.xml");
    

    //ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");

    PropertiesModel p = new PropertiesModel();
 	p.setTestmode("false");
 	p.setTestfile("httpckan.data.ktn.gv.atstoragef20140630T133A123A02.832Zsteuergem12.csv");
 	
 	p.setRemoveExistingBData("true");
 	p.setExecuteSQLqueries("false");
 	
 	p.setGeonamesdebugmode("false");
 	p.setFieldtypesdebugmode("false");
 	
 	p.setCsvfiles_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/");
 	p.setTmp_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/");
 	p.setProcessed_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/processed/");
 	p.setNewformat_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/processed/new_format/");
 	p.setEnriched_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/processed/enriched/");
 	p.setMissinggeoreference_dir("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/processed/discarded_files/");
 	p.setSqlinserts_file("/Users/david/Desktop/at_dump_v1/wwdagvat/tmp/processed/sql_inserts.sql");
 	
 	     	p.setNrowchecks("20");
 	     	p.setPvalue_nrowchecks("0.3");
 	     	p.setImageRegex(".*.(jpg|gif|png|bmp|ico)$");
 	     	p.setPhoneRegex("^\\\\+?[0-9. ()-]{10,25}$");
 	     	p.setCityRegex(".*[a-z]{3,30}.*");
 	     	p.setArchiveRegex(".*.(zip|7z|bzip(2)?|gzip|jar|t(ar|gz)|dmg)$");
 	     	p.setDocumentRegex(".*.(doc(x|m)?|pp(t|s|tx)|o(dp|tp)|pub|pdf|csv|xls(x|m)?|r(tf|pt)|info|txt|tex|x(ml|html|ps)|rdf(a|s)?|owl)$");
 	     	p.setOpeninghoursRegex("([a-z ]+ )?(mo(n(day)?)?|tu(e(s(day)?)?)?|we(d(nesday)?)?|th(u(r(s(day)\\u200C\\u200B?)?)?)?|fr(i(day)?)?\\u200C\\u200B|sa(t(urday)?)?|su(n\\u200C\\u200B(day)?)?)(-|:| ).*|([a-z ]+ )?(mo(n(tag)?)?|di(e(n(stag)?)?)?|mi(t(woch)?)?|do(n(er(s(tag)\\u200C\\u200B?)?)?)?|fr(i(tag)?)?\\u200C\\u200B|sa(m(stag)?)?|do(n(erstag)?)?)(-|:| ).*");
 	     	p.setDateRegex("([0-9]{2})?[0-9]{2}( |-|\\\\/|.)[0-3]?[0-9]( |-|\\\\/|.)([0-9]{2})?[0-9]{2}");
 	     	p.setYearRegex("^(?:18|20)\\\\d{2}$");
 	     	p.setCurrencyRegex("^(\\\\d+|\\\\d+[.,']\\\\d+)\\\\p{Sc}|\\\\p{Sc}(\\\\d+|\\\\d+[.,']\\\\d+)$");
 	     	p.setPercentageRegex("^(\\\\d+|\\\\d+[.,']\\\\d+)%|%(\\\\d+|\\\\d+[.,']\\\\d+)$");
 	     	p.setPostcodeRegex("^[0-9]{2}$|^[0-9]{4}$");
 	     	p.setNutsRegex("\\\\w{3,5}");
 	     	
 	     	p.setShapeRegex("point\\\\s*\\\\(([+-]?\\\\d+\\\\.?\\\\d+)\\\\s*,?\\\\s*([+-]?\\\\d+\\\\.?\\\\d+)\\\\)");
 	     	p.setLatitudeRegex("^-?([1-8]?[1-9]|[1-9]0)\\\\.{1}\\\\d{4,9}$");
 	     	p.setLongitudeRegex("^-?([1]?[1-7][1-9]|[1]?[1-8][0]|[1-9]?[0-9])\\\\.{1}\\\\d{4,9}$");
 	     	p.setLatlngRegex("([+-]?\\\\d+\\\\.?\\\\d+)\\\\s*,\\\\s*([+-]?\\\\d+\\\\.?\\\\d+)");
 	     	p.setPossiblenameRegex(".*[0-9]+.*");

 	p.setCountrycode("AT");
 	p.setShapes_file("/NUTS_2013_SHP/data/NUTS_RG_01M_2013.shp");

 	p.setGeonames_dbdriver("org.postgresql.Driver");
 	p.setGeonames_dburl("jdbc:postgresql://127.0.0.1:5432/geonames");
 	p.setGeonames_dbusr("postgres");
 	p.setGeonames_dbpwd("postgres");
 	
 	p.setWeb_dbdriver("web_dbdriver=org.postgresql.Driver");
 	p.setWeb_dburl("jdbc:postgresql://127.0.0.1:5432/spatialdatasearch");
 	p.setWeb_dbusr("postgres");
 	p.setWeb_dbpwd("postgres");

p.setSt1postcode("select p.name,admin3name,code,p.latitude,p.longitude,g.population,g.elevation from postalcodes p inner join geoname g on p.admin3 = g.admin3 where code like ? order by code asc;");
p.setSt2postcode("select p.name,admin3name,code,p.latitude,p.longitude,g.population,g.elevation from postalcodes p inner join geoname g on p.admin3 = g.admin3 where code = ? order by code asc;");
p.setSt1city("select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname = ? order by population desc;");
p.setSt2city("select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname like ? or asciiname like ? order by population desc;");
p.setSt3city("select geonameid,name,latitude,longitude,population,elevation from geoname where asciiname like ? or asciiname like ? order by population desc;");

    


try {
    entityManagerFactory = Persistence.createEntityManagerFactory("spatialdatasearch");
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    entityManager.getTransaction().begin();
    entityManager.persist(p);
    entityManager.getTransaction().commit();
    System.out.println("successfull");
    entityManager.close();
} catch (Exception e) {
    e.printStackTrace();
}
//entityManager.createQuery("delete from routes").executeUpdate();

    
    
    
    
    

    

    	
    try {
      Class.forName(geonames_dbdriver);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return;
    }

    try {
      conn = DriverManager.getConnection(geonames_dburl, geonames_dbusr, geonames_dbpwd);
    } catch (SQLException e) {
      log.info("Connection Failed! Check output console");
      e.printStackTrace();
      return;
    }

    if (conn != null) {
      log.info("Connected to DB !");
    } else {
      log.info("Failed to make DB connection!");
    }

    
    if (!executeSQLqueries) {

    log.info("Creating directories...");




    createDir(tmp_dir);
    createDir(processed_dir);
    createDir(newformat_dir);
    createDir(enriched_dir);
    createDir(missinggeoreference_dir);

    File folderToSearch = new File(csvfiles_dir);


    BufferedWriter bw_sql_inserts =
        new BufferedWriter(new FileWriter(new File(sqlinserts_file)));

    // PrintWriter out_sql_bestfiles_buffer = new PrintWriter(sql_bestfiles_buffer);
    // PrintWriter out_sql_nutsfiles_buffer = new PrintWriter(sql_nutsfiles_buffer);
    // PrintWriter out_sql_geofiles_buffer = new PrintWriter(sql_geofiles_buffer);

    log.info("Processing files...");

    nrowchecks += 1; // skip header

    log.info(">> MODE " + geonamesdebugmode);
    for (File file : folderToSearch.listFiles()) {

      name = file.getName();

      if (name.contains(".csv")) {

        if (testmode) {
          if (name.contentEquals(test_file)) {

            findFieldTypes(csvfiles_dir, name);
            GenerateCSVFiles.copyFile(file, new File(tmp_dir + name));
            processFile(file, new File(newformat_dir + name));
            GenerateSQLStatements.createSQLInserts(new File(newformat_dir + name),
                bw_sql_inserts);
          }
        }

        if (!testmode) {

          findFieldTypes(csvfiles_dir, name);
          GenerateCSVFiles.copyFile(file, new File(tmp_dir + name));
          processFile(file, new File(newformat_dir + name));
          GenerateSQLStatements.createSQLInserts(new File(newformat_dir + name),
              bw_sql_inserts);
        }
      }
    }

    bw_sql_inserts.close();
  }

    if (executeSQLqueries) {

      RunSqlScript.runSqlScript();
    }

    log.info("Finished! :)");
  }
}
