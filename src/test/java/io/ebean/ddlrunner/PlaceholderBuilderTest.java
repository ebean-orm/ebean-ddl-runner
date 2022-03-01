package io.ebean.ddlrunner;

import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PlaceholderBuilderTest {

  @Test
  void empty() {
    assertThat(PlaceholderBuilder.build(null, null)).isEmpty();
  }

  @Test
  void comma() {
    assertThat(PlaceholderBuilder.build("a=1", null))
      .containsExactly(MapEntry.entry("a", "1"));
  }

  @Test
  void comma_withSpace() {
    assertThat(PlaceholderBuilder.build(" a=1 ", null))
      .containsExactly(MapEntry.entry("a", "1"));
  }

  @Test
  void comma_withSpace_withSemi() {
    assertThat(PlaceholderBuilder.build(" a=1 ; ", null))
      .containsExactly(MapEntry.entry("a", "1"));
  }

  @Test
  void commaPair() {
    assertThat(PlaceholderBuilder.build("a=1;b=2", null))
      .containsExactly(MapEntry.entry("a", "1"), MapEntry.entry("b", "2"));
  }


  @Test
  void mapPair() {
    Map<String, String> in = new HashMap<>();
    in.put("a", "1");
    in.put("b", "2");

    assertThat(PlaceholderBuilder.build(null, in))
      .containsExactly(MapEntry.entry("a", "1"), MapEntry.entry("b", "2"));
  }

  @Test
  void mapPair_override() {
    Map<String, String> in = new HashMap<>();
    in.put("a", "1");
    in.put("b", "2");

    assertThat(PlaceholderBuilder.build("a=11;b=12", in))
      .containsExactly(MapEntry.entry("a", "1"), MapEntry.entry("b", "2"));
  }

  @Test
  void mapPair_join() {
    Map<String, String> in = new HashMap<>();
    in.put("c", "3");

    assertThat(PlaceholderBuilder.build("a=1;b=2", in))
      .containsExactly(MapEntry.entry("a", "1"), MapEntry.entry("b", "2"), MapEntry.entry("c", "3"));
  }
}
