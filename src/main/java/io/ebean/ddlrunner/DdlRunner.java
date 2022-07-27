package io.ebean.ddlrunner;

import io.avaje.applog.AppLog;

import java.io.StringReader;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Runs DDL scripts.
 */
public final class DdlRunner {

  static final System.Logger logger = AppLog.getLogger("io.ebean.DDL");

  private final DdlParser parser;
  private final String scriptName;
  private final boolean useAutoCommit;

  /**
   * Construct with a script name (for logging) and flag indicating if
   * auto commit is used (in which case errors are allowed).
   */
  public DdlRunner(boolean useAutoCommit, String scriptName) {
    this(useAutoCommit, scriptName, DdlDetect.NONE);
  }

  /**
   * Create specifying the database platform by name.
   */
  public DdlRunner(boolean useAutoCommit, String scriptName, String platformName) {
    this(useAutoCommit, scriptName, DdlDetect.forPlatform(platformName));
  }

  /**
   * Create specifying the auto commit behaviour (for the database platform).
   */
  public DdlRunner(boolean useAutoCommit, String scriptName, DdlDetect ddlAutoCommit) {
    this.useAutoCommit = useAutoCommit;
    this.scriptName = scriptName;
    this.parser = new DdlParser(this.useAutoCommit ? DdlDetect.NONE : ddlAutoCommit);
  }

  /**
   * Parse the content into sql statements and execute them in a transaction.
   *
   * @return The non-transactional statements that should execute later.
   */
  public List<String> runAll(String content, Connection connection) throws SQLException {
    List<String> statements = parser.parse(new StringReader(content));
    runStatements(statements, connection);
    return parser.getNonTransactional();
  }

  /**
   * Execute all the statements in a single transaction.
   */
  private void runStatements(List<String> statements, Connection connection) throws SQLException {
    boolean setAutoCommit = useAutoCommit && !connection.getAutoCommit();
    if (setAutoCommit) {
      connection.setAutoCommit(true);
    }
    try {
      logger.log(Level.INFO, "Executing {0} - {1} statements, autoCommit:{2}", scriptName, statements.size(), useAutoCommit);
      for (int x = 0, y = statements.size(); x < y; x++) {
        String xOfy = (x + 1) + " of " + y;
        String ddl = statements.get(x);
        runStatement(xOfy, ddl, connection);
      }
    } finally {
      if (setAutoCommit) {
        connection.setAutoCommit(false);
      }
    }
  }

  /**
   * Execute the statement.
   */
  private void runStatement(String oneOf, String stmt, Connection c) throws SQLException {
    // trim and remove trailing ; or /
    stmt = stmt.trim();
    if (stmt.endsWith(";")) {
      stmt = stmt.substring(0, stmt.length() - 1);
    } else if (stmt.endsWith("/")) {
      stmt = stmt.substring(0, stmt.length() - 1);
    }
    if (stmt.isEmpty()) {
      logger.log(Level.DEBUG, "skip empty statement at {0}", oneOf);
      return;
    }
    if (logger.isLoggable(Level.DEBUG)) {
      logger.log(Level.DEBUG, "executing {0} {1}", oneOf, summary(stmt));
    }

    try (PreparedStatement statement = c.prepareStatement(stmt)) {
      statement.execute();
    } catch (SQLException e) {
      if (useAutoCommit) {
        logger.log(Level.DEBUG, " ... ignoring error executing {0} error: {1}", summary(stmt), e.getMessage());
      } else {
        throw new SQLException("Error executing stmt[" + stmt + "] error[" + e.getMessage() + "]", e);
      }
    }
  }

  private String summary(String s) {
    if (s.length() > 80) {
      return s.substring(0, 80).trim().replace('\n', ' ') + "...";
    }
    return s.replace('\n', ' ');
  }

  /**
   * Run any non-transactional statements from the just parsed script.
   */
  public int runNonTransactional(Connection connection) {
    final List<String> nonTransactional = parser.getNonTransactional();
    return !nonTransactional.isEmpty() ? runNonTransactional(connection, nonTransactional) : 0;
  }

  /**
   * Run the non-transactional statements with auto commit true.
   */
  public int runNonTransactional(Connection connection, List<String> nonTransactional) {
    int count = 0;
    try {
      logger.log(Level.DEBUG, "running {0} non-transactional migration statements", nonTransactional.size());
      connection.setAutoCommit(true);
      for (String sql : nonTransactional) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          logger.log(Level.DEBUG, "executing - {0}", sql);
          statement.execute();
          count++;
        } catch (SQLException e) {
          logger.log(Level.ERROR, "Error running non-transaction migration: " + sql, e);
          return count;
        }
      }
      return count;
    } catch (SQLException e) {
      logger.log(Level.ERROR, "Error trying to run non-transaction migrations", e);
      return count;
    } finally {
      try {
        connection.setAutoCommit(false);
      } catch (SQLException e) {
        logger.log(Level.ERROR, "Error resetting connection autoCommit to false", e);
      }
    }
  }
}
