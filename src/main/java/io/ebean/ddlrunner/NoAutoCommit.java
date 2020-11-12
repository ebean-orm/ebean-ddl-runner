package io.ebean.ddlrunner;

/**
 * By default no statements require auto commit.
 */
class NoAutoCommit implements DdlAutoCommit {

  @Override
  public boolean transactional(String sql) {
    return true;
  }

  @Override
  public boolean isAutoCommit() {
    return false;
  }
}
