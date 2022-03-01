package io.ebean.ddlrunner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DdlAutoCommitTest {

  @Test
  void testForPlatform_postgres() {
    assertThat(DdlAutoCommit.forPlatform("postgres")).isInstanceOf(PostgresAutoCommit.class);
    assertThat(DdlAutoCommit.forPlatform("POSTGRES")).isInstanceOf(PostgresAutoCommit.class);
    assertThat(DdlAutoCommit.forPlatform("Postgres")).isInstanceOf(PostgresAutoCommit.class);
  }

  @Test
  void testForPlatform_cockroach() {
    assertThat(DdlAutoCommit.forPlatform("cockroach")).isInstanceOf(CockroachAutoCommit.class);
    assertThat(DdlAutoCommit.forPlatform("COCKROACH")).isInstanceOf(CockroachAutoCommit.class);
  }

  @Test
  void testForPlatform_others() {
    assertThat(DdlAutoCommit.forPlatform("other")).isInstanceOf(NoAutoCommit.class);
    assertThat(DdlAutoCommit.forPlatform("mysql")).isInstanceOf(NoAutoCommit.class);
    assertThat(DdlAutoCommit.forPlatform("oracle")).isInstanceOf(NoAutoCommit.class);
  }
}
