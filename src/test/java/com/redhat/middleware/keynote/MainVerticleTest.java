package com.redhat.middleware.keynote;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MainVerticleTest {

  private Vertx vertx;
  private int maxConnections = 10;
  private int maxHit = 100;
  private int maxGrace;

  @Test
  public void testStringFormat() {
    System.out.println(LogUtils.format("Hello %1$s", "vert.x"));
  }

  @Before
  public void setUp() throws InterruptedException {
    maxConnections = 10;
    maxHit = 100;
    maxGrace = 2000;
    AtomicBoolean done = new AtomicBoolean();
    Vertx.clusteredVertx(new VertxOptions(), v -> {
      vertx = v.result();
      vertx.deployVerticle(MainVerticle.class.getName(), ar -> {
        done.set(ar.succeeded());
      });
    });
    await().untilAtomic(done, is(true));
  }

  @After
  public void tearDown() {
    AtomicBoolean done = new AtomicBoolean();
    vertx.close(ar -> done.set(ar.succeeded()));
    await().untilAtomic(done, is(true));
  }

  @Test
  public void testWithSmallNumberOfFastClients() {
    maxConnections = 10;
    maxHit = 100;
    maxGrace = 100;
    run();
  }

  @Test
  public void testWithSmallNumberOfVeryFastClients() {
    maxConnections = 50;
    maxHit = 1000;
    maxGrace = 10;
    run();
  }

  @Test
  public void testWithSmallNumberOfVeryVeryFastClients() {
    maxConnections = 100;
    maxHit = 1000;
    maxGrace = 1;
    run();
  }

  @Test
  @Ignore
  public void testWithLoad() {
    maxConnections = 1000;
    maxHit = 500;
    maxGrace = 200;
    run();
  }


  public void run() {
    int millis = (int) (50.0 / maxConnections * 1000);
    if (millis > maxGrace) {
      millis = maxGrace;
    }
    List<String> errors = new ArrayList<>();
    Map<String, String> idsToUserName = new HashMap<>();
    System.out.println("Running test - " + millis);
    AtomicInteger global = new AtomicInteger();

    JsonObject score = new JsonObject().put("type", "score");
    long begin = System.currentTimeMillis();
    for (int i = 0; i < maxConnections; i++) {
      HttpClient client = vertx.createHttpClient();
      client.websocket(9001, "localhost", "/game", socket -> {
        socket.exceptionHandler(Throwable::printStackTrace);
        AtomicInteger counter = new AtomicInteger();
        socket.handler(buffer -> {
          JsonObject json = buffer.toJsonObject();
          if (json.getString("type") == null) {
            errors.add("No type in " + json.encode());
            return;
          }
          if (json.getString("type").equals("id")) {
            if (json.getString("id") == null) {
              errors.add("Missing id in id frame " + json.encode());
            }
          }
          if (json.getString("type").equals("configuration")) {
//            if (json.getString("playerId") == null || json.getString("playerId").isEmpty()) {
//              errors.add("Invalid playerId in " + json.encode());
//              return;
//            }
//            if (json.getInteger("team", -1) <= 0) {
//              errors.add("Invalid team in " + json.encode());
//              return;
//            }
//            if (json.getInteger("score", -1) < 0) {
//              errors.add("Invalid score in " + json.encode());
//              return;
//            }
//            if (json.getString("username") == null || json.getString("username").isEmpty()) {
//              errors.add("Invalid username in " + json.encode());
//              return;
//            }
//
//            String username = idsToUserName.get(json.getString("playerId"));
//            if (username == null) {
//              idsToUserName.put(json.getString("playerId"), json.getString("username"));
//            } else {
//              if (!username.equals(json.getString("username"))) {
//                errors.add("Invalid id - name in " + json.encode());
//              }
//            }
          } else if (json.getString("type").equals("team-score")) {
            if (json.getInteger("score", -1) < 0) {
              errors.add("Invalid team score in " + json.encode());
            }
          } else if (json.getString("type").equals("state")) {
            if (json.getString("state") == null) {
              errors.add("Invalid state in " + json.encode());
            }
          }
        });
        vertx.setPeriodic(getRandomInt(100, maxGrace), l -> {
          if (counter.get() >= maxHit) {
            vertx.cancelTimer(l);
            client.close();
          } else {
            counter.incrementAndGet();
            global.incrementAndGet();
            socket.writeFinalTextFrame(score.encode());
          }
        });
      });
      grace(millis);
    }
    System.out.println("All clients created after " + (System.currentTimeMillis() - begin)
        + " ms");

    vertx.setPeriodic(5000, l -> {
      System.out.println("Number of events " + global.get() + " / " + maxConnections * maxHit);
    });

    await().atMost(5, TimeUnit.MINUTES)
        .until(() -> global.get() >= maxConnections * maxHit);
    assertThat(errors).isEmpty();
    long end = System.currentTimeMillis();
    System.out.println(end - begin + " ms");
  }

  private void grace(int millis) {
    try {
      Thread.sleep(getRandomInt(100, millis));
    } catch (Exception e) {
      // Ignore it
    }
  }

  private long getRandomInt(int min, int max) {
    return (long) Math.floor(Math.random() * (max - min)) + min;
  }
}