package com.redhat.middleware.keynote;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.redhat.middleware.keynote.GameUtils.Endpoint;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.TimeoutStream;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Counter;

public class ScoreTimerVerticle extends AbstractVerticle {

  private static final String SUMMARY_REQUEST_PREFIX = "{" +
      "\"lookup\"   : \"SummarySession\"," +
      "\"commands\" : [" +
      "  { \"insert\" : {" +
      "       \"object\" : {\"com.redhatkeynote.score.ScoreSummary\":{" +
      "         \"topPlayers\"     : ";
  private static final String SUMMARY_REQUEST_SUFFIX = "       }}," +
      "       \"out-identifier\" : \"scoreSummary\"," +
      "       \"return-object\" : true" +
      "    }" +
      "  }," +
      "  {" +
      "      \"fire-all-rules\" : {}" +
      "  } ]" +
      "}";

  private Map<Integer, Counter> teamCounters = new ConcurrentHashMap<>();
  private Map<Integer, Counter> teamPopCounters = new ConcurrentHashMap<>();
  private Endpoint achievementEndpoint ;
  private HttpClient achievementClient;
  private HttpClientOptions scoreClientOptions;

  @Override
  public void start(Future<Void> future) throws Exception {
    final long interval = config().getLong("interval", 500L);
    final int numTopPlayers = config().getInteger("numTopPlayers", 10);
    final int testPort = config().getInteger("innerPort", 9002);
    final int numTeams = config().getInteger("number-of-teams", 4);

    achievementEndpoint = GameUtils.retrieveEndpoint("ACHIEVEMENTS_SERVER", testPort, "/testAchievementServer");
    final Endpoint scoreEndpoint = GameUtils.retrieveEndpoint("SCORE_SERVER", testPort, "/testScoreServer");
    String scoreUser = System.getenv("SCORE_USER");
    String scorePassword = System.getenv("SCORE_PASSWORD");
    final String scoreAuthHeader ;
    if ((scoreUser != null) && (scorePassword != null)) {
      scoreAuthHeader = "Basic " + Base64.getEncoder().encodeToString((scoreUser.trim() + ':' + scorePassword.trim()).getBytes("UTF-8"));
    } else {
      scoreAuthHeader = null;
    }

    // Should only need 1
    scoreClientOptions = new HttpClientOptions().setMaxPoolSize(20);

    // Should only need 1
    final HttpClientOptions achievementClientOptions = new HttpClientOptions().setMaxPoolSize(20);
    achievementClient = vertx.createHttpClient(achievementClientOptions);

    List<Future> futures = new ArrayList<>();
    futures.addAll(getTeamCounters("redhat.team", teamCounters, numTeams));
    futures.addAll(getTeamCounters("redhat.team.pop", teamPopCounters, numTeams));

    CompositeFuture.all(futures).setHandler(ar -> {
      if (ar.succeeded()) {
        final TimeoutStream periodicStream = vertx.periodicStream(interval);
        periodicStream.handler(time -> {
          periodicStream.pause();
          HttpClient scoreClient = vertx.createHttpClient(scoreClientOptions);
          final HttpClientRequest scoreRequest = scoreClient.post(scoreEndpoint.getPort(), scoreEndpoint.getHost(), scoreEndpoint.getPath(), resp -> {
            resp.exceptionHandler(t -> {
              t.printStackTrace();
              scoreClient.close();
              periodicStream.resume();
            });
            if (resp.statusCode() == 200) {
              resp.bodyHandler(body -> {
                List<Future> sendFutures = new ArrayList<>();
                final JsonObject response = body.toJsonObject();
                if ((response != null) && "SUCCESS".equals(response.getString("type"))) {
                  final JsonObject scoreSummaryResult = findScoreSummary(response);

                  final JsonObject teamScoreSummary = walkTree(scoreSummaryResult, "com.redhatkeynote.score.ScoreSummary");
                  if (teamScoreSummary != null) {
                    final JsonArray teamScores = teamScoreSummary.getJsonArray("teamScores");
                    sendFutures.add(sendTeamScores(teamScores, numTeams));

                    final JsonArray topPlayerScores = teamScoreSummary.getJsonArray("topPlayerScores");
                    sendFutures.add(sendTopPlayerScores(topPlayerScores));
                  }
                }
                if (sendFutures.size() > 0) {
                  CompositeFuture.all(sendFutures).setHandler(result -> {
                    scoreClient.close();
                    periodicStream.resume();
                  });
                } else {
                  scoreClient.close();
                  periodicStream.resume();
                }
              });
            } else {
              scoreClient.close();
              System.out.println("Received error response from Score endpoint: " + resp.statusMessage());
              periodicStream.resume();
            }
          })
              .putHeader("Accept", "application/json")
              .putHeader("Content-Type", "application/json")
              .setTimeout(10000)
              .exceptionHandler(t -> {
                t.printStackTrace();
                scoreClient.close();
                periodicStream.resume();
              });
          if (scoreAuthHeader != null) {
            scoreRequest.putHeader("Authorization", scoreAuthHeader);
          }
          scoreRequest.end(SUMMARY_REQUEST_PREFIX + numTopPlayers + SUMMARY_REQUEST_SUFFIX);
        });
        future.complete();
      } else {
        ar.cause().printStackTrace();
        future.fail(ar.cause());
      }
    });
  }

  private JsonObject findScoreSummary(JsonObject response) {
    final JsonObject executionResults = walkTree(response, "result", "execution-results");
    if (executionResults != null) {
      final JsonArray results = executionResults.getJsonArray("results");
      if (results != null) {
        for (int index = 0 ; index < results.size(); index++) {
          final JsonObject result = results.getJsonObject(index);
          if ("scoreSummary".equals(result.getString("key"))) {
            return result.getJsonObject("value");
          }
        }
      }
    }
    return null;
  }

  private static JsonObject walkTree(JsonObject tree, String ... paths) {
    if (paths != null) {
      for(String path: paths) {
        tree = tree.getJsonObject(path);
      }
      return tree;
    }
    return null;
  }

  private Future sendTeamScores(JsonArray teamScores, final int numTeams) {
    final List<Future> futures = new ArrayList<>();
    final int teamCount = teamScores.size();
    final List<JsonObject> orderedTeamScores = new ArrayList<>();
    for(int index = 0 ; index < numTeams ; index++) {
      final JsonObject teamScore = new JsonObject();
      teamScore.put("team", index+1);
      teamScore.put("score", 0);
      teamScore.put("numPlayers", 0);
      teamScore.put("numPops", 0);
      orderedTeamScores.add(teamScore);
    }

    for(int index = 0 ; index < teamCount ; index++) {
      JsonObject teamScore = teamScores.getJsonObject(index);
      final Integer team = teamScore.getInteger("team");
      orderedTeamScores.set(team-1, teamScore);
    }

    for(int index = 0 ; index < numTeams ; index++) {
      JsonObject teamScore = orderedTeamScores.get(index);
      final Integer team = teamScore.getInteger("team");
      Counter teamCounter = teamCounters.get(team);
      if (teamCounter != null) {
        Future teamCounterFuture = Future.future();
        futures.add(teamCounterFuture);
        teamCounter.get(result -> {
          if (result.succeeded()) {
            teamScore.put("numPlayers", result.result().intValue());
          }
          teamCounterFuture.complete();
        });
      } else {
        System.out.println("Could not find teamCounter for team: " + teamCounter);
      }
      Counter teamPopCounter = teamPopCounters.get(team);
      if (teamPopCounter != null) {
        Future teamPopCounterFuture = Future.future();
        futures.add(teamPopCounterFuture);
        teamPopCounter.get(result -> {
          if (result.succeeded()) {
            teamScore.put("numPops", result.result().intValue());
          }
          teamPopCounterFuture.complete();
        });
      } else {
        System.out.println("Could not find teamPopCounter for team: " + teamCounter);
      }
    }

    if (futures.size() > 0) {
      Future future = Future.future();
      CompositeFuture.all(futures).setHandler( ar -> {
        if (ar.succeeded()) {
          vertx.eventBus().publish("/scores", new JsonArray(orderedTeamScores));
        }
        future.complete();
      });
      return future;
    } else {
      return Future.succeededFuture();
    }
  }

  private Future sendTopPlayerScores(JsonArray topPlayerScores) {
    final List<Future> futures = new ArrayList<>();
    final int playerCount = topPlayerScores.size();
    for(int index = 0 ; index < playerCount ; index++) {
      JsonObject playerScore = topPlayerScores.getJsonObject(index);
      final String uuid = playerScore.getString("uuid");

      final Future future = Future.future();
      futures.add(future);

      String path = achievementEndpoint.getPath() + "/achievement/" + uuid;
      final HttpClientRequest scoreRequest = achievementClient.get(achievementEndpoint.getPort(), achievementEndpoint.getHost(), path, resp -> {
        resp.exceptionHandler(t -> {
          t.printStackTrace();
          future.complete();
        });
        if (resp.statusCode() == 200) {
          resp.bodyHandler(body -> {
            final JsonArray achievementResponse = body.toJsonArray();
            final JsonObject achievements = new JsonObject();
            for(int count = 0 ; count < achievementResponse.size(); count++) {
              final JsonObject achievement = achievementResponse.getJsonObject(count);
              achievements.put(achievement.getString("type"), true);
            }
            playerScore.put("achievements", achievements);
            future.complete();
          });
        } else {
          System.out.println("Received error response from Achievement endpoint: " + resp.statusMessage());
          future.complete();
        }
      })
          .putHeader("Accept", "application/json")
          .putHeader("Content-Type", "application/json")
          .setTimeout(10000)
          .exceptionHandler(t -> {
            t.printStackTrace();
            future.complete();
          });
      scoreRequest.end();
    }
    Future future = Future.future();
    CompositeFuture.all(futures).setHandler( ar -> {
      if (ar.succeeded()) {
        vertx.eventBus().publish("/leaders", topPlayerScores);
      }
      future.complete();
    });
    return future;
  }

  private List<Future> getTeamCounters(final String namePrefix, final Map<Integer, Counter> counters, final int numTeams) {
    final List<Future> futures = new ArrayList<>();
    for(int team = 1 ; team <= numTeams ; team++) {
      final Future future = Future.future();
      futures.add(future);
      String name = namePrefix + "." + team;
      final Integer teamKey = team;
      vertx.sharedData().getCounter(name, ar -> {
        counters.put(teamKey, ar.result());
        if (ar.succeeded()) {
          future.complete();
        } else {
          future.fail(ar.cause());
        }
      });
    }
    return futures;
  }
}
