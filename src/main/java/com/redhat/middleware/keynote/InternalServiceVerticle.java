package com.redhat.middleware.keynote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class InternalServiceVerticle extends AbstractVerticle {

  /**
   * Not immutable for testing purpose...
   */
  public static String defaultConfiguration = "{\"opacity\": 85, \"scale\": 0.3, \"speed\": 50, \"background\": " +
      "\"default\", " +
      "\"points\": { \"red\": 1, \"blue\": 1, \"green\": 1, \"yellow\": 1, \"goldenSnitch\": 50 }, \"goldenSnitch\": false, \"trafficPercentage\": 100}";

  public static String defaultScore = "{ \"type\" : \"SUCCESS\", \"msg\" : \"Container score_70e71112d291851c17a1214b5a650837 successfully called.\","
                            + "\"result\" : { \"execution-results\" : { \"results\" : [ { \"key\" : \"newAchievements\","
                            + "\"value\" : {\"com.redhatkeynote.score.AchievementList\":{ \"achievements\" : [ { \"type\" : \"pops1\","
                            + "\"description\" : \"Splash Expert! 10 in a row!\" }, { \"type\" : \"score1\", \"description\" : \"Splish Splash! 10 points!\" },"
                            + "{ \"type\": \"golden\", \"description\": \"Burr Balloon! You popped it!\" } ] }} } ],"
                            + "\"facts\" : [ { \"key\" : \"newAchievements\", \"value\" : {\"org.drools.core.common.DefaultFactHandle\":{"
                            + "\"external-form\" : \"0:2:1004942150:1004942150:2:DEFAULT:NON_TRAIT:com.redhatkeynote.score.AchievementList\" }} } ] } } }";

  public static String defaultScoreSummary = "{ \"type\" : \"SUCCESS\", \"msg\" : \"Container score_70e71112d291851c17a1214b5a650837 successfully called.\", "
          + "\"result\" : { \"execution-results\" : { \"results\" : [ { \"key\" : \"scoreSummary\", \"value\" : {\"com.redhatkeynote.score.ScoreSummary\":{ "
          + "\"teamScores\" : [ { \"team\" : 1, \"score\" : 500, \"numPlayers\" : 1 } ], \"topPlayerScores\" : [ { \"uuid\" : \"p1\", \"username\" : \"John Doe\", \"score\" : 500 } ],"
          + " \"topPlayers\" : 10 }} } ], \"facts\" : [ { \"key\" : \"scoreSummary\", \"value\" : {\"org.drools.core.common.DefaultFactHandle\":{"
          + " \"external-form\" : \"0:1:1512740648:1512740648:1:DEFAULT:NON_TRAIT:com.redhatkeynote.score.ScoreSummary\" }} } ] } } }";

  public static String defaultAchievements = "[{" +
    "\"type\": \"TOP_SCORE\"," +
    "\"description\": \"The TOP_SCORE achievement\"" +
    "}]";

  @Override
  public void start(Future<Void> future) throws Exception {
    Router internalRouter = Router.router(vertx);
    internalRouter.route().handler(BodyHandler.create());

    internalRouter.post("/configurationUpdated").handler(this::configurationUpdated);
    internalRouter.get("/testMechanicsServer").handler(this::testMechanicsServer);
    // internalRouter.putWithRegex("/testAchievementServer/achievement/update/.*").handler(this::testAchievementServerUpdate);
    internalRouter.getWithRegex("/testAchievementServer/achievement/.*").handler(this::testAchievementServerAchievement);
    internalRouter.delete("/testAchievementServer/reset").handler(this::testAchievementServerReset);
    internalRouter.post("/testScoreServer").handler(this::testScoreServer);
    internalRouter.post("/updateScores").handler(this::updateScores);

    vertx.createHttpServer()
        .requestHandler(internalRouter::accept)
        .listen(config().getInteger("innerPort", 9002), ar -> {
          if (ar.succeeded()) {
            System.out.println("Game server internal endpoints started on 0.0.0.0:" + ar.result().actualPort());
            future.complete();
          } else {
            System.out.println("Internal endpoints failed, cause: " + ar.cause());
            future.fail(ar.cause());
          }
        });
  }

  private void configurationUpdated(RoutingContext context) {
    JsonObject json = new JsonObject().put("event", "configurationUpdated");
    vertx.eventBus().publish("configurationUpdated", json);
    context.response().setStatusCode(200).end("OK");
  }

  private void testMechanicsServer(RoutingContext context) {
    System.out.println("Returning default test configuration");
    context.response().end(defaultConfiguration);
  }

  private void testAchievementServerUpdate(RoutingContext context) {
    System.out.println("Received Achievement on path " + context.request().path());
    context.response().setStatusCode(200).end();
  }

  private void testAchievementServerAchievement(RoutingContext context) {
    // System.out.println("Returning default achievements");
    context.response().end(defaultAchievements);
  }

  private void testAchievementServerReset(RoutingContext context) {
    System.out.println("Received Achievement reset on path " + context.request().path());
    context.response().setStatusCode(204).end();
  }

  private void testScoreServer(RoutingContext context) {
    final JsonObject body = context.getBodyAsJson();
    final String lookup = (String) body.getValue("lookup");
    if ("SummarySession".equals(lookup)) {
        context.response().end(defaultScoreSummary);
    } else {
        context.response().end(defaultScore);
    }
  }

  private void updateScores(RoutingContext context) {
    vertx.eventBus().publish("/scores", context.getBodyAsJsonArray());
    context.response().setStatusCode(200).end("OK");
  }
}
