package com.redhat.middleware.keynote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class BoardVerticle extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    vertx.createHttpServer()
        .requestHandler(req -> {
          req.response().end();
        })
        .websocketHandler(ws -> {
          if (ws.path().equals("/leaderboard")) {
            onLeaderBoard(ws);
          } else if (ws.path().equals("/scoreboard")) {
            onScoreBoard(ws);
          } else {
            ws.reject();
          }
        }).listen(config().getInteger("boardPort", 9004), ar -> {
      System.out.println("Boards HTTP server started on port " + ar.result().actualPort());
    });
  }

  private void onScoreBoard(ServerWebSocket ws) {
    // This WS is used to push the score periodically
    MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer("/scores");
    ws.closeHandler(v -> consumer.unregister());
    consumer.handler(message -> ws.writeFinalTextFrame(message.body().encode()));
  }


  private void onLeaderBoard(ServerWebSocket ws) {
    MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer("/leaders");
    ws.closeHandler(v -> consumer.unregister());
    consumer.handler(message -> ws.writeFinalTextFrame(message.body().encode()));
  }
}
