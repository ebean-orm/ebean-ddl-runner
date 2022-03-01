package io.ebean.ddlrunner;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScriptTransformTest {

  @Test
  void transform() {
    Map<String, String> map = new HashMap<>();
    map.put("one", "PLACE1");
    map.put("two", "PLACE2");

    ScriptTransform transform = new ScriptTransform(map);

    assertEquals(transform.transform("${one}"), "PLACE1");
    assertEquals(transform.transform("${two}"), "PLACE2");
    assertEquals(transform.transform("${one}${two}"), "PLACE1PLACE2");
    assertEquals(transform.transform("${two}${one}"), "PLACE2PLACE1");
    assertEquals(transform.transform("A${one}B${two}C"), "APLACE1BPLACE2C");
    assertEquals(transform.transform(" ${one} ${two} "), " PLACE1 PLACE2 ");

    assertEquals(transform.transform("$${one}"), "$PLACE1");
    assertEquals(transform.transform("$${one}}"), "$PLACE1}");
  }
}
