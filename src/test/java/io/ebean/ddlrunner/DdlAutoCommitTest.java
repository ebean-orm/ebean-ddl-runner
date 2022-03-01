package io.ebean.ddlrunner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DdlAutoCommitTest {

  @Test
  void testForPlatform_postgres() {
    assertThat(DdlDetect.forPlatform("postgres")).isInstanceOf(DdlDetectPostgres.class);
    assertThat(DdlDetect.forPlatform("POSTGRES")).isInstanceOf(DdlDetectPostgres.class);
    assertThat(DdlDetect.forPlatform("Postgres")).isInstanceOf(DdlDetectPostgres.class);
  }

  @Test
  void testForPlatform_others() {
    assertThat(DdlDetect.forPlatform("other")).isInstanceOf(DdlDetectNone.class);
    assertThat(DdlDetect.forPlatform("mysql")).isInstanceOf(DdlDetectNone.class);
    assertThat(DdlDetect.forPlatform("oracle")).isInstanceOf(DdlDetectNone.class);
  }
}
