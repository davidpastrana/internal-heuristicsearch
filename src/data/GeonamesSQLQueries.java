package data;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeonamesSQLQueries extends MainClass {

  private final static Logger log = LoggerFactory.getLogger(GeonamesSQLQueries.class);

  public static void setGeonameResult(LocationModel loc, ResultSet rs)
      throws NumberFormatException, SQLException {

    loc.setLatitude(new BigDecimal(rs.getString(3)));
    loc.setLongitude(new BigDecimal(rs.getString(4)));
    loc.setPopulation(rs.getString(5));
    loc.setElevation(rs.getString(6));
  }

  public static void DebugInfo(PreparedStatement ps, ResultSet rs) throws SQLException {
    log.info(ps.toString());
    log.info(rs.getString(2) + ",  " + rs.getString(3) + "," + rs.getString(4));
  }

  public static void DebugError(PreparedStatement ps, ResultSet rs) throws SQLException {
    log.info(ps.toString());
    log.info("--------------------------------- NO Resul found ---------------------------------");
  }

  public static ResultSet getPostcodeLatLng(Integer postcode, Connection conn) throws SQLException {

    PreparedStatement ps = null;
    Statement st = conn.createStatement();
    st.setMaxRows(1);
    ResultSet rs = null;

    if (postcode > 0 && postcode < 10000) {
      if (postcode < 100) {
        ps = conn.prepareStatement(st1postcode);
        ps.setString(1, "_" + String.valueOf(postcode) + "_");
        rs = ps.executeQuery();

        if (!rs.next()) {
          if (geonamesdebugmode)
            DebugError(ps, rs);
          return null;
        } else {
          if (geonamesdebugmode)
            DebugInfo(ps, rs);
          return rs;
        }
      } else {
        ps = conn.prepareStatement(st2postcode);
        ps.setString(1, String.valueOf(postcode));
        rs = ps.executeQuery();

        if (!rs.next()) {
          if (geonamesdebugmode)
            DebugError(ps, rs);
          return null;
        } else {
          if (geonamesdebugmode)
            DebugInfo(ps, rs);
          return rs;
        }
      }
    }

    return null;

  }

  public static ResultSet getCityLatLng(String name, Connection conn) throws SQLException {

    PreparedStatement ps = null;
    Statement st = conn.createStatement();
    st.setMaxRows(1);
    ResultSet rs = null;

    if (geonamesdebugmode)
      log.info("\nchecking > " + name);

    name = name.toLowerCase().replaceAll("\"|(.)?[0-9](.)?|'|^\\s*$", "").trim();

    // log.info("NUMBER OF WORDS: " + name.split(" ").length);

    // we get the first part from / division
    if (name.contains("/")) {
      name = name.split("/")[0];
    }

    // replace anything between parenthesis and replace German characters with _
    name =
        name.replaceAll("\\(.*?\\) ?", "").replace("ü", "ue").replace("ö", "oe").replace("ä", "ae")
            .replace("ß", "ss").replace("Ü", "ue").replace("Ö", "oe").replace("Ä", "ae").trim();


    if (name != null && !name.isEmpty() && !name.matches("\\d+")) {

      ps = conn.prepareStatement(st1city);
      ps.setString(1, name);
      rs = ps.executeQuery();

      if (name.length() >= 4) {

        // if there is no resultset
        if (!rs.next()) {

          // we remove dots
          if (name.contains(".")) {
            name = name.replace(".", "");
          }

          // we will query with the last 80% of chars of the city
          int nchars = (name.length() * 80) / 100;
          ps = conn.prepareStatement(st2city);
          ps.setString(1, name.substring(0, 1) + "%" + name.substring(name.length() - nchars));
          ps.setString(2, name + "%");
          rs = ps.executeQuery();

          if (!rs.next()) {

            // we get the first part from - division
            if (name.contains("-")) {
              name = name.replace("-", " ");
              name = name.replaceAll("\\s+", " ").trim(); // remove more than one empty space
            }

            String firstTwoWord = "";
            String lastTwoWord = "";
            String[] values = name.split(" ");

            // if it has more than 2 words
            if (values.length > 2) {
              firstTwoWord = values[0] + "%" + values[1];
              lastTwoWord = values[values.length - 2] + "%" + values[values.length - 1];
            } else if (name.length() > 6) {
              // we get the first and last characters (60%) of the word
              nchars = (name.length() * 60) / 100;
              firstTwoWord = name.substring(0, nchars);
              lastTwoWord = name.substring(name.length() - nchars);
            } else {
              return null;
            }

            // log.info("first words/chars: " + firstTwoWord);
            // log.info("last words/chars: " + lastTwoWord);

            ps = conn.prepareStatement(st3city);
            ps.setString(1, "%" + firstTwoWord + "%");
            ps.setString(2, "%" + lastTwoWord + "%");
            rs = ps.executeQuery();

            if (!rs.next()) {
              if (geonamesdebugmode)
                DebugError(ps, rs);
              return null;
            } else {
              if (geonamesdebugmode)
                DebugInfo(ps, rs);
              return rs;
            }
          } else {
            if (geonamesdebugmode)
              DebugInfo(ps, rs);
            return rs;
          }
        } else {
          if (geonamesdebugmode)
            DebugInfo(ps, rs);
          return rs;

        }
      }
    }
    return null;
  }
}
