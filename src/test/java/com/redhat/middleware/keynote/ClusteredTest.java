package com.redhat.middleware.keynote;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ClusteredTest {

  private List<Vertx> nodes = new ArrayList<>();

  @Before
  public void setup() {
    AtomicBoolean done = new AtomicBoolean();

    Future fut1 = Future.future();
    startNode(fut1);

    fut1.setHandler(ar -> {
      done.set(true);
    });

    await().atMost(1, TimeUnit.MINUTES).untilAtomic(done, is(true));
  }

  @After
  public void tearDown() {
    for (Vertx v : nodes) {
      AtomicBoolean done = new AtomicBoolean();
      v.close(ar -> {
        done.set(true);
      });
      await().untilAtomic(done, is(true));
    }
  }


  @Test
  public void testRepartitionAmongTheDifferentTeams() throws InterruptedException {
    int users = 50;

    Map<Integer, AtomicInteger> count = new LinkedHashMap<>();
    for (int i = 1; i < 5; i++) {
      count.put(i, new AtomicInteger());
    }

    Vertx vertx = Vertx.vertx();
    nodes.add(vertx);
    for (int i = 0; i < users; i++) {
      HttpClient client = vertx.createHttpClient();
      int id = i;
      client.websocket(9001, "localhost", "/game", socket -> {
        socket.frameHandler(frame -> {
          System.out.println("User " + id + " receives " + frame.textData());
          JsonObject json = new JsonObject(frame.textData());
          Integer team = json.getInteger("team", -1);
          if (team != -1) {
            count.get(team).incrementAndGet();
          }
        });
        socket.writeFinalTextFrame("{}");
      });
    }

    await().until(() -> count.values().stream().mapToInt(AtomicInteger::get).sum() == users);

    assertThat(count.get(1).get()).isBetween(12, 13);
    assertThat(count.get(2).get()).isBetween(12, 13);
    assertThat(count.get(3).get()).isBetween(12, 13);
    assertThat(count.get(4).get()).isBetween(12, 13);
  }

  @Test
  public void testWithExistingUsers() throws InterruptedException {
    int users = 50;

    Map<Integer, AtomicInteger> count = new ConcurrentHashMap<>();
    for (int i = 1; i < 5; i++) {
      count.put(i, new AtomicInteger());
    }

    List<JsonObject> registrations = new ArrayList<>();
    int team = 0;
    for (int i = 0; i < users; i++) {
      registrations.add(new JsonObject()
          .put("id", UUID.randomUUID().toString())
          .put("team", (team % 3) + 1)
          .put("username", UserNameGenerator.generate()));
      team++;
    }

    Vertx vertx = Vertx.vertx();
    nodes.add(vertx);
    for (int i = 0; i < users; i++) {
      HttpClient client = vertx.createHttpClient();
      int id = i;
      client.websocket(9001, "localhost", "/game", socket -> {
        socket.frameHandler(frame -> {
          System.out.println("User " + id + " receive " + frame.textData());
          JsonObject json = new JsonObject(frame.textData());
          Integer assignedTeam = json.getInteger("team", -1);
          if (assignedTeam != -1) {
            count.get(assignedTeam).incrementAndGet();
          }
        });
        socket.writeFinalTextFrame(registrations.get(id).encode());
      });
    }

    await().until(() -> count.values().stream().mapToInt(AtomicInteger::get).sum() == users);

    assertThat(count.get(1).get()).isBetween(10, 17);
    assertThat(count.get(2).get()).isBetween(10, 17);
    assertThat(count.get(3).get()).isBetween(10, 17);
    assertThat(count.get(4).get()).isBetween(10, 17);
  }


  private void startNode(Future future) {
    JsonObject config = new JsonObject()
        .put("game-verticle-instances", 3)
        .put("server-verticle-instances", 3)
        .put("internal-server-verticle-instances", 3);

    Vertx.clusteredVertx(new VertxOptions(),
        ar -> {
          Vertx vertx = ar.result();
          nodes.add(vertx);
          vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions().setConfig(config),
              future.completer());
        });
  }

  @Test
  public void testThatConfigurationIsPushedToEveryone() throws InterruptedException {
    int users = 50;

    Map<Integer, JsonObject> cfg = new ConcurrentHashMap<>();
    AtomicInteger userCount = new AtomicInteger();
    Vertx vertx = Vertx.vertx();
    nodes.add(vertx);
    for (int i = 0; i < users; i++) {
      HttpClient client = vertx.createHttpClient();
      int id = i;
      client.websocket(9001, "localhost", "/game", socket -> {
        socket.frameHandler(frame -> {
          System.out.println("User " + id + " receives " + frame.textData());
          JsonObject json = new JsonObject(frame.textData());
          String type = json.getString("type");
          if (type != null && type.equalsIgnoreCase("configuration")) {
            cfg.put(id, json);
          }
          Integer assignedTeam = json.getInteger("team", -1);
          if (assignedTeam != -1) {
            userCount.incrementAndGet();
          }
        });
        socket.writeFinalTextFrame("{}");
      });
    }

    await().until(() -> userCount.get() == users);
    await().until(() -> cfg.size() == users);


    cfg.clear();

    // Send new configuration
    InternalServiceVerticle.defaultConfiguration = "{\"opacity\": 85, \"scale\": 0.3, \"speed\": 50, \"background\": \"default\", \"points\": { \"red\": 1, \"blue\": 1, \"green\": 1, \"yellow\": 1, \"goldenSnitch\": 50 }, \"goldenSnitch\": true}";

    vertx.createHttpClient().post(9002, "localhost", "/configurationUpdated", response -> {
      System.out.println(response.statusCode());
    }).end();

    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      System.out.println(cfg.size());
      return cfg.size() == users;
    });

    cfg.entrySet().forEach(entry -> {
      if (entry.getValue() == null || !entry.getValue().getJsonObject("configuration")
          .getBoolean("goldenSnitch", false)) {
        fail("Invalid configuration for user " + entry.getKey() + " " + entry.getValue());
      }
    });

    // Restore the configuration
    InternalServiceVerticle.defaultConfiguration = "{\"opacity\": 85, \"scale\": 0.3, \"speed\": 50, \"background\": \"default\", \"points\": { \"red\": 1, \"blue\": 1, \"green\": 1, \"yellow\": 1, \"goldenSnitch\": 50 }, \"goldenSnitch\": false}";

  }

  @Test
  public void testThatConfigurationIsPushedToEveryoneWhenClientLeave() throws InterruptedException {
    int users = 50;

    Map<Integer, WebSocket> sockets = new ConcurrentHashMap<>();
    Map<Integer, JsonObject> cfg = new ConcurrentHashMap<>();
    AtomicInteger userCount = new AtomicInteger();
    Vertx vertx = Vertx.vertx();
    nodes.add(vertx);
    for (int i = 0; i < users; i++) {
      HttpClient client = vertx.createHttpClient();
      int id = i;
      client.websocket(9001, "localhost", "/game", socket -> {
        socket.frameHandler(frame -> {
          System.out.println("User " + id + " receive " + frame.textData());
          JsonObject json = new JsonObject(frame.textData());
          String type = json.getString("type");
          if (type != null && type.equalsIgnoreCase("configuration")) {
            cfg.put(id, json);
          }
          Integer assignedTeam = json.getInteger("team", -1);
          if (assignedTeam != -1) {
            userCount.incrementAndGet();
          }
        });
        socket.writeFinalTextFrame("{}");
        sockets.put(id, socket);
      });
    }

    await().until(() -> userCount.get() == users);
    await().until(() -> cfg.size() == users);
    await().until(() -> sockets.size() == users);

    AtomicInteger close = new AtomicInteger();
    sockets.entrySet().forEach(entry -> {
      if (entry.getKey() % 2 == 0) {
        entry.getValue().closeHandler(v -> {
          System.out.println("Closed " + entry.getKey());
          close.incrementAndGet();
        });
        entry.getValue().close();
      }
    });

    await().untilAtomic(close, is(25));

    cfg.clear();

    // Send new configuration
    InternalServiceVerticle.defaultConfiguration = "{\"opacity\": 85, \"scale\": 0.3, \"speed\": 50, \"background\": \"default\", \"points\": { \"red\": 1, \"blue\": 1, \"green\": 1, \"yellow\": 1, \"goldenSnitch\": 50 }, \"goldenSnitch\": true}";

    vertx.createHttpClient().post(9002, "localhost", "/configurationUpdated", response -> {
      System.out.println(response.statusCode());
    }).end();

    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      System.out.println(cfg.size());
      return cfg.size() == users / 2;
    });

    cfg.entrySet().forEach(entry -> {
      if (entry.getValue() == null || !entry.getValue().getJsonObject("configuration")
          .getBoolean("goldenSnitch", false)) {
        fail("Invalid configuration for user " + entry.getKey() + " " + entry.getValue());
      }
    });

    // Restore the configuration
    InternalServiceVerticle.defaultConfiguration = "{\"opacity\": 85, \"scale\": 0.3, \"speed\": 50, \"background\": \"default\", \"points\": { \"red\": 1, \"blue\": 1, \"green\": 1, \"yellow\": 1, \"goldenSnitch\": 50 }, \"goldenSnitch\": false}";

  }
}
