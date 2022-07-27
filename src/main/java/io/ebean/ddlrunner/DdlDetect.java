package io.ebean.ddlrunner;

/**
 * Detect non-transactional SQL statements that must run after normal
 * sql statements with connection set to auto commit true.
 */
public interface DdlDetect {

  DdlDetect NONE = new DdlDetectNone();

  DdlDetect POSTGRES = new DdlDetectPostgres();

  /**
   * Return the implementation for the given platform.
   */
  static DdlDetect forPlatform(String name) {
    return "postgres".equalsIgnoreCase(name) ? POSTGRES : NONE;
  }

  /**
   * Return false if the SQL is non-transactional and should run with auto commit.
   */
  boolean transactional(String sql);

}
