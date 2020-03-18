package io.andygrove.ballista.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

/**
 * JDBC Driver unit tests.
 */
public class ResultSetHelperTest {

  //TODO add exhaustive tests based on suggested conversions in JDBC specification

  @Test
  public void getString() throws SQLException {
    assertNull(ResultSetHelper.getString(null));
    assertEquals("a", ResultSetHelper.getString("a"));
    assertEquals("123", ResultSetHelper.getString(123));
  }

  @Test
  public void getInt() throws SQLException {
    assertEquals(0, ResultSetHelper.getInt(null));
    assertEquals(123, ResultSetHelper.getInt("123"));
    assertEquals(123, ResultSetHelper.getInt(123));
  }

}
