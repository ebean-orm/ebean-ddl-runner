package io.ebean.ddlrunner;

import java.util.regex.Pattern;

/**
 * Postgres requires create/drop index concurrently to run with auto commit true.
 */
final class DdlDetectPostgres implements DdlDetect {

  private static final Pattern IX_CONCURRENTLY = Pattern.compile(Pattern.quote(" index concurrently "), Pattern.CASE_INSENSITIVE);

  @Override
  public boolean transactional(String sql) {
    return !IX_CONCURRENTLY.matcher(sql).find();
  }

}
