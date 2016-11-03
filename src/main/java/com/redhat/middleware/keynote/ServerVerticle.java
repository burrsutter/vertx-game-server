package com.redhat.middleware.keynote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Counter;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import static com.redhat.middleware.keynote.LogUtils.format;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ServerVerticle extends AbstractVerticle {

  public final static Logger LOGGER = LoggerFactory.getLogger(ServerVerticle.class);
  private Counter activeUserCounter;

  @Override
  public void start(Future<Void> future) throws Exception {
    Router router = Router.router(vertx);

    router.route().handler(BodyHandler.create());
    router.route().handler(CorsHandler.create("*"));

    router.get("/health").handler(this::ping);

    vertx.createHttpServer()
        .websocketHandler(ws -> {
          switch (ws.path()) {
            case "/game/admin":
              onAdminConnection(ws);
              break;
            case "/game":
              onPlayerConnection(ws);
              break;
            default:
              LOGGER.warn(format("Connection to web socket %1$s not supported", ws.path()));
              ws.reject();
          }
        })
        .requestHandler(router::accept)
        .listen(config().getInteger("port", 9001), ar -> {
          if (ar.succeeded()) {
            LOGGER.info(format("Game server started on 0.0.0.0:%1$d", ar.result().actualPort()));

            vertx.sharedData().getCounter("redhat.users", r -> {
              activeUserCounter = r.result();
              future.complete();
            });

          } else {
            future.fail(ar.cause());
          }
        });
  }


  private void manageWebSocket(String announceAddress, ServerWebSocket socket) {
    socket
        .exceptionHandler(Throwable::printStackTrace)
        .frameHandler(frame -> {
          //LOGGER.info(format("===> %1$s", frame.textData()));
          // This frame handler is only there for the initial message (registration), then another frame handler will
          // replace it.
          vertx.eventBus().<String>send(announceAddress,
              new JsonObject().put("event", "message").put("message", toJson(frame)),
              new DeliveryOptions().setSendTimeout(30000),
              ar -> {
                if (ar.failed()) {
                  LOGGER.error("Rejecting web socket connection", ar.cause());
                  closeQuietly(socket);
                } else {
                  // Get the id of the player / admin
                  String id = ar.result().body();
                  String address = id + "/message";
                  MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(id);
                  // Burr
                  // Once we have the address used for the communication with the game verticle, replace the frame
                  // handler. Now it just delegates to the event bus
                  socket
                      .frameHandler(innerframe -> {
                        vertx.eventBus().send(address,
                            new JsonObject().put("event", "message")
                                .put("message", toJson(innerframe)));
                      })
                      .exceptionHandler(t -> {
                        cleanupConnection(address, consumer);
                        closeQuietly(socket);
                      })
                      .closeHandler(v -> {
                        cleanupConnection(address, consumer);
                      });

                  // Register the consumer receiving message from the game verticle to write to the socket
                  consumer.handler(message -> {
                    // Message sent from the game verticle to be transferred to the web socket
                    try {
                      socket.writeFinalTextFrame(message.body().encode());
                    } catch (IllegalStateException e) {
                      // Socket closed.
                      cleanupConnection(address, consumer);
                      closeQuietly(socket);
                    }
                  }).completionHandler(x -> {
                      // Everything is setup, send the id to the user.
                      JsonObject json = new JsonObject().put("type", "id").put("id", id);
                      //LOGGER.debug(format("<==== %1$s", json.encode()));
                      socket.writeFinalTextFrame(json.encode());

                      vertx.eventBus().send(address,
                          new JsonObject().put("event", "init"));

                      activeUserCounter.addAndGet(1, v -> {
                      });
                  });
                }
              });
        });
  }

  private void closeQuietly(ServerWebSocket socket) {
    try {
      socket.close();
    } catch (Exception ignored) {

    }
  }

  private void cleanupConnection(String address, MessageConsumer<JsonObject> consumer) {
    consumer.unregister();
    vertx.eventBus().send(address, new JsonObject().put("event", "gone"));
    activeUserCounter.addAndGet(-1, x -> {
    });
  }

  private void onAdminConnection(ServerWebSocket socket) {
    manageWebSocket("admin", socket);
  }

  private void onPlayerConnection(ServerWebSocket socket) {
    manageWebSocket("player", socket);
  }

  private JsonObject toJson(WebSocketFrame frame) {
    // We only expect text frame.
    return new JsonObject(frame.textData());
  }

  private void ping(RoutingContext context) {
    activeUserCounter.get(l -> {
      if (l.succeeded()) {
        context.response().setStatusCode(200).end("OK " + l.result() + " active users");
      } else {
        context.response().setStatusCode(200).end("KO");
      }
    });

  }
}
