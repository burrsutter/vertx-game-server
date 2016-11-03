package com.redhat.middleware.keynote;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class UserNameGeneratorTest {

  @Test
  public void testGenerationOfNames() {
    Set<String> names = new HashSet<>();
    for(int i = 0; i < 50000; i++) {
      names.add(UserNameGenerator.generate());
    }

    assertThat(names).hasSize(50000);
    assertThat(names).doesNotContain("", null);
  }

}