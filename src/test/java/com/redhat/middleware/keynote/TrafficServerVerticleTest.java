package com.redhat.middleware.keynote;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@RunWith(VertxUnitRunner.class)
public class TrafficServerVerticleTest {

  private Vertx vertx;
  private HttpClient client1, client2;

  @Before
  public void setup(TestContext tc) {
    AtomicBoolean done = new AtomicBoolean();

    Vertx.clusteredVertx(new VertxOptions(),
        v -> {
          vertx = v.result();
          vertx.exceptionHandler(tc.exceptionHandler());
          vertx.deployVerticle(TrafficServerVerticle.class.getName(),
              new DeploymentOptions().setInstances(3),
              ar -> {
                done.set(true);
              });
        });

    await().atMost(1, TimeUnit.MINUTES).untilAtomic(done, is(true));
  }

  @Test
  public void testTraffic(TestContext tc) {

    List<JsonArray> list = new CopyOnWriteArrayList<>();

    AtomicBoolean startReceiving = new AtomicBoolean();

    client1 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        System.out.println("Receiving " + frame.textData());
        if (!frame.textData().equals("[]")) {
          list.add(new JsonArray(frame.textData()));
        }
        startReceiving.set(true);
      });
    });

    await().untilAtomic(startReceiving, is(true));

    vertx.runOnContext(v -> {
      vertx.eventBus().send("traffic", new JsonObject().put("to", "1").put("from", "2"));
      vertx.eventBus().send("traffic", new JsonObject().put("to", "2").put("from", "2"));
      vertx.eventBus().send("traffic", new JsonObject().put("to", "3").put("from", "4"));
      vertx.eventBus().send("traffic", new JsonObject().put("to", "1").put("from", "2"));
    });


    await().until(() -> {
      AtomicInteger size = new AtomicInteger();
      list.forEach(x -> size.addAndGet(x.size()));
      return size.get() == 4;
    });
  }

  @Test
  public void testTrafficFromPost(TestContext tc) {

    List<JsonArray> list = new CopyOnWriteArrayList<>();

    AtomicBoolean startReceiving = new AtomicBoolean();

    client1 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        System.out.println("Receiving " + frame.textData());
        if (!frame.textData().equals("[]")) {
          list.add(new JsonArray(frame.textData()));
        }
        startReceiving.set(true);
      });
    });

    await().untilAtomic(startReceiving, is(true));


    client2 = vertx.createHttpClient();
    client2.post(9003, "localhost", "/record", resp -> {
    })
        .end(new JsonObject().put("to", "1").put("from", "2").encode());
    client2.post(9003, "localhost", "/record", resp -> {
    })
        .end(new JsonObject().put("to", "2").put("from", "2").encode());
    client2.post(9003, "localhost", "/record", resp -> {
    })
        .end(new JsonObject().put("to", "3").put("from", "4").encode());
    client2.post(9003, "localhost", "/record", resp -> {
    })
        .end(new JsonObject().put("to", "1").put("from", "2").encode());


    await().until(() -> {
      AtomicInteger size = new AtomicInteger();
      list.forEach(x -> size.addAndGet(x.size()));
      return size.get() == 4;
    });
  }

  @Test
  public void testTrafficFromWS(TestContext tc) {

    List<JsonArray> list = new CopyOnWriteArrayList<>();

    AtomicBoolean startReceiving = new AtomicBoolean();

    client1 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        if (!frame.textData().equals("[]")) {
          list.add(new JsonArray(frame.textData()));
        }
        startReceiving.set(true);
      });
    });

    await().untilAtomic(startReceiving, is(true));


    client2 = vertx.createHttpClient().websocket(9003, "localhost", "/record", ws -> {
      ws.writeFinalTextFrame(new JsonObject().put("to", "1").put("from", "2").encode());
      ws.writeFinalTextFrame(new JsonObject().put("to", "2").put("from", "2").encode());
      ws.writeFinalTextFrame(new JsonObject().put("to", "3").put("from", "4").encode());
      ws.writeFinalTextFrame(new JsonObject().put("to", "1").put("from", "2").encode());
    });

    await().until(() -> {
      AtomicInteger size = new AtomicInteger();
      list.forEach(x -> size.addAndGet(x.size()));
      return size.get() == 4;
    });
  }

  @Test
  public void testTrafficFromWSWithSplitFrame(TestContext tc) {

    List<JsonArray> list = new CopyOnWriteArrayList<>();

    AtomicBoolean startReceiving = new AtomicBoolean();

    client1 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        if (!frame.textData().equals("[]")) {
          list.add(new JsonArray(frame.textData()));
        }
        startReceiving.set(true);
      });
    });

    await().untilAtomic(startReceiving, is(true));


    client2 = vertx.createHttpClient().websocket(9003, "localhost", "/record", ws -> {
      String s = "{\"to\":1,";
      String s2 ="\"from\":2}";
      ws.writeFrame(WebSocketFrame.textFrame(s, false));
      ws.writeFrame(WebSocketFrame.continuationFrame(Buffer.buffer(s2), true));


      s = "{\"to\":3,";
      s2 ="\"from\":4}";
      ws.writeFrame(WebSocketFrame.textFrame(s + s2, false));
      ws.writeFrame(WebSocketFrame.continuationFrame(Buffer.buffer(), true));

      s = "{\"to\":5,";
      s2 ="\"from\":6}";
      ws.writeFrame(WebSocketFrame.textFrame("", false));
      ws.writeFrame(WebSocketFrame.continuationFrame(Buffer.buffer(s), false));
      ws.writeFrame(WebSocketFrame.continuationFrame(Buffer.buffer(s2), true));
    });

    await().until(() -> {
      AtomicInteger size = new AtomicInteger();
      list.forEach(x -> size.addAndGet(x.size()));
      return size.get() == 3;
    });

    System.out.println(list);
  }

  @Test
  public void testTrafficWithSomeLoad(TestContext tc) throws InterruptedException {

    int entries = 50000;

    List<JsonArray> list1 = new CopyOnWriteArrayList<>();
    List<JsonArray> list2 = new CopyOnWriteArrayList<>();

    AtomicBoolean startReceiving = new AtomicBoolean();
    AtomicBoolean startReceiving2 = new AtomicBoolean();


    client1 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        if (!frame.textData().equals("[]")) {
          list1.add(new JsonArray(frame.textData()));
        }
        startReceiving.set(true);
      });
    });

    client2 = vertx.createHttpClient().websocket(9003, "localhost", "/stream", ws -> {
      ws.frameHandler(frame -> {
        if (!frame.textData().equals("[]")) {
          list2.add(new JsonArray(frame.textData()));
        }
        startReceiving2.set(true);
      });
    });

    await().untilAtomic(startReceiving, is(true));
    await().untilAtomic(startReceiving2, is(true));


    for (int t = 0; t < 10; t++) {
      new Thread(() -> {
        for (int i = 0; i < entries / 10; i++) {
          int to = (int) (Math.random() * 10);
          int from = (int) (Math.random() * 10);
          JsonObject traffic = new JsonObject().put("to", Integer.toString(to))
              .put("from", Integer.toString(from));
          int delay = (int) (Math.random() * 10);
          if (delay == 0) {
            delay = 1;
          }
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          vertx.eventBus().publish("traffic", traffic);
        }
      }).start();
    }

    await().atMost(1, TimeUnit.MINUTES).until(() -> {
      AtomicInteger size1 = new AtomicInteger();
      AtomicInteger size2 = new AtomicInteger();
      list1.forEach(x -> size1.addAndGet(x.size()));
      list2.forEach(x -> size2.addAndGet(x.size()));
      System.out.println(size1.get() + " / " + size2);
      return size1.get() == entries && size2.get() == entries;
    });
  }

  @After
  public void tearDown() {
    if (client1 != null) {
      client1.close();
    }
    if (client2 != null) {
      client2.close();
    }

    AtomicBoolean done = new AtomicBoolean();
    vertx.close(ar -> {
      done.set(true);
    });
    await().untilAtomic(done, is(true));
  }
}
