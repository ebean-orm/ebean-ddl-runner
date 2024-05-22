package io.ebean.ddlrunner;

import io.ebean.datasource.DataSourcePool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DdlRunnerTest {

  String sql = "create table junk0 (acol varchar(10))";

  String sql2 = "create table junk1 (acol varchar(10));" +
    "\ncreate table junk2 (id integer);" +
    "\ncreate table junk3 (id integer);";

  private static DataSourcePool ds = dataSource();

  @BeforeAll
  static void before() {
    ds = dataSource();
  }

  @AfterAll
  static void after() {
    ds.shutdown();
  }

  @Test
  void run() throws SQLException {
    DdlRunner runner = new DdlRunner(true, "test");
    try (Connection connection = ds.getConnection()) {
      List<String> nonTransactional = runner.runAll(sql, connection);
      assertThat(nonTransactional).isEmpty();
    }
  }

  @Test
  void run3() throws SQLException {
    DdlRunner runner = new DdlRunner(false, "test", "h2");
    try (Connection connection = ds.getConnection()) {
      List<String> nonTransactional = runner.runAll(sql2, connection);
      assertThat(nonTransactional).isEmpty();
      connection.commit();

      int count = runner.runNonTransactional(connection);
      assertThat(count).isEqualTo(0);
    }
  }

  private static DataSourcePool dataSource() {
    return DataSourcePool.builder()
      .name("")
      .username("sa")
      .password("")
      .url("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      .build();
  }

}
