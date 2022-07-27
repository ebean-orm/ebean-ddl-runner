package io.ebean.ddlrunner;

/**
 * By default no statements require auto commit.
 */
final class DdlDetectNone implements DdlDetect {

  @Override
  public boolean transactional(String sql) {
    return true;
  }

}
