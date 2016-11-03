package com.redhat.middleware.keynote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class TrafficServerVerticle extends AbstractVerticle {

  private Buffer buffer = Buffer.buffer();

  @Override
  public void start(Future<Void> future) throws Exception {
    int port = config().getInteger("trafficPort", 9003);
    long period = config().getLong("trafficPingInterval", 2000L);


    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route().handler(CorsHandler.create("*"));

    router.get("/health").handler(this::ping);
    router.post("/record").handler(this::addTraffic);

    vertx.createHttpServer()
        .requestHandler(router::accept)
        .websocketHandler(socket -> {
              switch (socket.path()) {
                case "/stream":
                  // Push traffic data.
                  MessageConsumer consumer = vertx.eventBus()
                      .<JsonObject>consumer("traffic")
                      .handler(m -> socket.writeFinalTextFrame(new JsonArray().add(m.body()).encode()));

                  // Keep the socket open.
                  long task = vertx.setPeriodic(period, l -> socket.writeFinalTextFrame("[]"));

                  socket.closeHandler(v -> {
                    vertx.cancelTimer(task);
                    consumer.unregister();
                  });
                  break;
                case "/record":
                  socket.frameHandler(frame -> {
                    buffer.appendString(frame.textData());
                    if (!frame.isFinal()) {
                      System.out.println("Non final frame sent from javascript ! " + frame);
                    } else {
                      vertx.eventBus().publish("traffic", buffer.toJsonObject());
                      buffer = Buffer.buffer();
                    }
                  });
                  break;
                default:
                  socket.reject();
              }
            }
        ).listen(port, done -> {
          if (done.succeeded()) {
            future.complete();
          } else {
            future.fail(done.cause());
          }
        }
    );
  }


  private void addTraffic(RoutingContext routingContext) {
    JsonObject json = routingContext.getBodyAsJson();
    vertx.eventBus().publish("traffic", json);
    routingContext.response().end();
  }


  private void ping(RoutingContext routingContext) {
    routingContext.response().end("OK");
  }
}
