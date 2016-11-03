import com.redhat.middleware.keynote.GameUtils
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.Json
import io.vertx.groovy.core.eventbus.EventBus
import io.vertx.groovy.core.eventbus.Message
import io.vertx.groovy.core.http.HttpClient
import io.vertx.groovy.core.shareddata.AsyncMap
import io.vertx.lang.groovy.GroovyVerticle
import java.util.Map
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;

class GameVerticle extends GroovyVerticle {

  // Configuration and constants
  public static final Map<String, Serializable> DEFAULT_CONF = [
          'opacity'          : 85,
          'scale'            : 0.3F,
          'speed'            : 50,
          'background'       : "default",
          'points'           : [
                  'red'         : 1,
                  'blue'        : 1,
                  'green'       : 1,
                  'yellow'      : 1,
                  'goldenSnitch': 50
          ],
          'goldenSnitch'     : false,
          'trafficPercentage': 100
  ]

  private static final Map<String, Map<String, Player>> TEAM_PLAYERS = new HashMap<>();

  static final String TEAM_COUNTER_NAME = "redhat.team";
  static final String TEAM_POP_COUNTER_NAME = "redhat.team.pop";
  static final String PLAYER_NAME_MAP = "redhat.player.name";

  // Configuration
  int num_teams
  int score_broadcast_interval

  String configurationHost
  int configurationPort
  String configurationPath

  String achievementHost
  int achievementPort
  String achievementPath

  String scoreHost
  int scorePort
  String scorePath
  String scoreAuthHeader

  // Verticle fields

  // Shared
  Map<Integer, Team> teams = [:];
  // Shared
  List<Admin> admins = [];

  def counter;
  def teamCounters = [:];
  def teamPopCounters = [:];

  Map adminConfiguration = [:]

  AsyncMap<String, String> playerNames;
  AsyncMap<String, Integer> playerScores; // BURR


  HttpClient mechanicsClient;
  HttpClient achievementClient;
  HttpClient scoreClient;

/*
 * Possible States
 * - title
 * - demo
 * - play
 * - pause
 * - game-over
 */

  /*
   * Possible Selfie States
   */
  // SHARED
  def state = 'title';
  def selfieState = 'closed';

  def authToken = 'CH2UsJePthRWTmLI8EY6';

  @Override
  public void start(Future<Void> future) throws Exception {
    num_teams = (int) context.config().get("number-of-teams", 4)
    score_broadcast_interval = (int) context.config().get("score-broadcast-interval", 2500)
    teams = Team.createTeams(num_teams)

    mechanicsClient = vertx.createHttpClient()
    achievementClient = vertx.createHttpClient([
            'maxPoolSize': 100
    ])
    scoreClient = vertx.createHttpClient([
            'maxPoolSize': 100
    ])

    setConfiguration(DEFAULT_CONF)

    def testPort = (int) context.config().get("innerPort", 9002);

    (configurationHost, configurationPort, configurationPath) = retrieveEndpoint("MECHANICS_SERVER", testPort, "/testMechanicsServer");
    println("Mechanics Server host: " + configurationHost + ", port " + configurationPort + ", path: " + configurationPath)

    (achievementHost, achievementPort, achievementPath) = retrieveEndpoint("ACHIEVEMENTS_SERVER", testPort, "/testAchievementServer");
    println("Achievement Server host: " + achievementHost + ", port " + achievementPort + ", path: " + achievementPath)

    (scoreHost, scorePort, scorePath) = retrieveEndpoint("SCORE_SERVER", testPort, "/testScoreServer");
    def scoreUser = System.getenv("SCORE_USER")?.trim()
    def scorePassword = System.getenv("SCORE_PASSWORD")?.trim()
    if (scoreUser && scorePassword) {
      scoreAuthHeader = "Basic " + Base64.getEncoder().encodeToString((scoreUser + ':' + scorePassword).getBytes("UTF-8"))
    }
    println("Score Server host: " + scoreHost + ", port " + scorePort + ", path: " + scorePath)

    EventBus eventBus = vertx.eventBus();

    // Receive configuration from the eventbus
    eventBus.<Map> consumer("configuration", {
      message ->
        Map configuration = message.body();
        setConfiguration(configuration);
    })

    // A configuration has been pushed, retrieve it from the server.
    eventBus.consumer("configurationUpdated", onConfigurationUpdated())

    // New player
    eventBus.consumer("player", onNewPlayer(eventBus))
    // New admin
    eventBus.consumer("admin", onNewAdmin(eventBus))
    // State change
    eventBus.consumer("state-change", onStateChange(eventBus))
    // Selfie State change
    eventBus.consumer("selfie-state-change", onSelfieStateChange(eventBus))

    // Ask for scores
    eventBus.consumer("/scores", updateTeamScores())

    def futures = []
    futures.add(getTeamCounter())
    futures.addAll(getIndividualTeamCounters())
    futures.addAll(getIndividualTeamPopCounters())
    futures.add(getPlayerNameMap())
    

    CompositeFuture.all(futures).setHandler({ ar ->
      if (ar.succeeded()) {
        future.complete();
      } else {
        ar.cause().printStackTrace();
        future.fail(ar.cause());
      }
    })

    retrieveConfiguration()
  }

  // control the state of the game
  // a front-end client with buttons will pass along the current state
  // of the game. states can be: title, demo, play, pause, game-over
  // the updated state will be broadcasted to all connected player clients
  // and to all admin clients so all connections will by in sync. this module,
  // game.js will also hold the state of the game so anytime a new player
  // connects, they will receive the current state of the game in the
  // onPlayerConnection method
  Handler<Message> onNewAdmin(EventBus eventBus) {
    { m ->
      Admin admin = new Admin();
      admins.add(admin);
      def consumer = eventBus.<Map> consumer(admin.userId + "/message")
      consumer.handler({ msg ->
        Map message = msg.body();
        def messageEvent = message['event']
        if (messageEvent == "gone") {
          consumer.unregister()
          admins.remove(admin)
          if (admin.ping != -1) vertx.cancelTimer(admin.ping)
        } else if (messageEvent == "init") {
          send(admin, [
                  type         : 'configuration',
                  configuration: adminConfiguration
          ]);

          send(admin, [
                  type : 'state',
                  state: state
          ]);

          send(admin, [
                  type : 'selfie-state',
                  state: selfieState
          ]);

          admin.ping = vertx.setPeriodic(10000, { l ->
            send(admin, [
                    type: 'heartbeat'
            ])
          })
        } else {
          Map data = message['message']

          // a matching token is required for any changes
          if (!data.token || data.token != authToken) {
            send(admin, [
                    type: 'auth-failed'
            ])
          } else {
            if (data.type == "state-change") {
              // KEV - validate data
              // Clear everything first then
              if (data.state == "start-game") {
                def future = resetAll()
                future.setHandler({ ar ->
                  if (ar.succeeded()) {
                    publish("state-change", data);
                    data.state = "play"
                    publish("state-change", data);
                  } else {
                    ar.cause().printStackTrace();
                  }
                })
              } else {
                publish("state-change", data);
              }
            } else if (data.type == "selfie-state-change") {
              // KEV - validate data
              publish("selfie-state-change", data);
            } else if (data.type == "configuration") {
              // KEV - validate data
              publish("configuration", data.configuration);
            }
          }
        }
      }).completionHandler({ x ->
        m.reply(admin.userId)
      });
    }
  }

  private Handler<Message<Map>> onNewPlayer(eventBus) {
    {
      m ->
        def message = m.body()['message'];
        String id = message.id
        if (id) {
          playerNames.get(id, { ar ->
            if (ar.succeeded()) {
              String name = ar.result();
              initializePlayerTeam(id, name, m, eventBus);
            } else {
              initializePlayerTeam(null, null, m, eventBus);
            }
          });
        } else {
          initializePlayerTeam(null, null, m, eventBus);
        }
    }
  }

  private void initializePlayerTeam(String id, String name, Message<Map> m, EventBus eventBus) {
    def message = m.body()['message'];
    Team team = (message.team ? teams.get(message.team.toInteger()) : null);
    if (id && name && team) {
      initializePlayer(id, name, team, m, eventBus)
    } else {
      // Pick the team and increment counter
      counter.addAndGet(1, { nar ->
        int assignment = (nar.result() % num_teams) + 1
        team = teams.get(assignment)
        initializePlayer(id, name, team, m, eventBus)
      });
    }
  }

  private void initializePlayer(String id, String name, Team team, Message<Map> m, EventBus eventBus) {
    Player player;

    if (id && name) {
      player = new Player(id, team, name);
    } else {
      player = new Player(team);
      playerNames.put(player.userId, player.username, { ar ->
        if (ar.failed()) {
          println("Put of player name in async map failed, id " + player.userId + ", name " + player.username)
        }
      })
    }
    def teamCounter = teamCounters[team.number]
    if (teamCounter) {
      teamCounter.incrementAndGet({ ar -> });
    } else {
      println("Unexpected team number: " + team.number);
    }

    def consumer = eventBus.<Map> consumer(player.userId + "/message")
    consumer.handler({ msg ->
      def event = msg.body()["event"]
      if (event == "gone") {
        consumer.unregister();
        team.players.remove(player);
        teamCounter?.decrementAndGet({ ar -> });
      } else if (event == "init") {
        send(player, [
                type         : 'configuration',
                team         : player.team.number,
                playerId     : player.userId,
                username     : player.username,
                score        : team.score,
                configuration: team.configuration
        ])

        send(player, [
                type : 'state',
                state: state
        ])

        send(player, [
                type : 'selfie-state',
                state: selfieState
        ])
      } else {
        Map message = msg.body()["message"];
        if (message.type == 'score') {
          // per ballon popped
          int score = message.getOrDefault('score', 0)
          
          int consecutive = message.getOrDefault('consecutive', 0)
          boolean goldenSnitchPopped = message.getOrDefault('goldenSnitchPopped', false)
                    
          updateATeamScore(player, score, consecutive, goldenSnitchPopped);

          // old way with BRMS
          // sendScore(player, score, consecutive, goldenSnitchPopped);
          
          teamPopCounters[team.number]?.addAndGet(1, {});

        } else {
          println("Unknown message type : " + message.type + " / " + message);
        }
      }
    }).completionHandler({ x ->
      team.players.add(player)
      m.reply(player.userId)
    });
  }

  // BURR
  private void updateATeamScore(Player player, int score, int consecutivePops, boolean goldenSnitchPopped) {
    
    System.out.println("Player: " + player.username + " ");
    System.out.println("Player id: " + player.userId + " ");
    System.out.println("Score: " + score + " ");
    System.out.println("Teamd ID: " + player.team.number + " ");
    System.out.println("Team Score: " + player.team.score + " ");
    System.out.println("Consecutive: " + consecutivePops);
    
    int deltaScore = 0;
    
    vertx.sharedData().getClusterWideMap("PLAYER_SCORES", { ar -> 
      if (ar.failed()) {
        System.out.println("FAILED getClusterWideMap");        
      } else {
        AsyncMap<String,Integer> playerScores = ar.result();
        System.out.println("AsyncMap acquired");
        Integer oldScore = playerScores.getAt(player.userId);
        deltaScore = score - oldScore.intValue();
        System.out.println("deltaScore: " + deltaScore)

        playerScores.putAt(player.userId, score)
      }
    });
  }

  private Handler<Message> onConfigurationUpdated() {
    { msg ->
      println("Received notification about configuration updates")
      retrieveConfiguration();
    }
  }

  def retrieveConfiguration() {
    println("Gonna try to retrieve the configuration")

    mechanicsClient.get(configurationPort, configurationHost, configurationPath, { resp ->
      resp.exceptionHandler({ t ->
        t.printStackTrace();
      });

      if (resp.statusCode() == 200) {
        resp.bodyHandler { body ->
          Map configuration = Json.decodeValue(body.toString(), Map.class);
          println("Retrieved configuration: " + configuration)
          setConfiguration(configuration);
        }
      } else {
        println("Received error response from Configuration endpoint");
      }
    })
            .setTimeout(10000)
            .exceptionHandler({ t ->
      t.printStackTrace()
    }).end();
  }

  // send score to score server, if there are any achievements returned then send them to achievement server plus client
  def sendScore(Player player, int score, int consecutivePops, boolean goldenSnitchPopped) {
    def currentAggregatedScore = player.aggregatedScore.get();
    if (currentAggregatedScore == null) {
      // If the new aggregate is null then a request is in progress
      def aggregatedScore = new AggregatedScore(score, consecutivePops, goldenSnitchPopped)
      if (!player.aggregatedScore.compareAndSet(currentAggregatedScore, aggregatedScore)) {
        // The state has changed so retry
        sendScore(player, score, consecutivePops, goldenSnitchPopped)
      }
    } else {
      def aggregatedScore = new AggregatedScore(
              score > currentAggregatedScore.score ? score : currentAggregatedScore.score,
              consecutivePops > currentAggregatedScore.consecutivePops ? consecutivePops : currentAggregatedScore.consecutivePops,
              goldenSnitchPopped ? true : currentAggregatedScore.goldenSnitchPopped
      )
      if (!player.aggregatedScore.compareAndSet(currentAggregatedScore, aggregatedScore)) {
        // The state has changed so retry
        sendScore(player, score, consecutivePops, goldenSnitchPopped)
      } else if (currentAggregatedScore.isDefaulted()) {
        // If the previous aggregate has default scores then the previous request has finished so we restart
        processSend(player)
      }
    }
  }

  def processSend(Player player) {
    def aggregatedScore = player.aggregatedScore.getAndSet(null)

    retryableProcessSend(0, player, aggregatedScore, Future.future())
  }

  def retryProcessSend(int attempt, Player player, AggregatedScore aggregatedScore, Future future) {
    int newAttempt = attempt + 1;
    if (newAttempt > 10) {
      println("Number of attempts reached " + attempt + ", cancelling")
      future.complete()
    } else {
      vertx.setTimer(1000, { x -> retryableProcessSend(newAttempt, player, aggregatedScore, future) })
    }
  }

  def retryableProcessSend(int attempt, Player player, aggregatedScore, Future future) {
    def String uuid = player.userId
    def String username = player.username
    def int team = player.team.number

    def playerUpdate = '{' +
            '  "lookup"   : "ScoreSession",' +
            '  "commands" : [' +
            '  { "insert" : {' +
            '      "object" : {"com.redhatkeynote.score.Player":{' +
            '         "uuid"     : "' + uuid + '",' +
            '         "username" : "' + username + '",' +
            '         "team"     : ' + team + ',' +
            '         "score"    : ' + aggregatedScore.score + ',' +
            '         "consecutivePops"     : ' + aggregatedScore.consecutivePops + "," +
            '         "goldenSnitch"     : ' + aggregatedScore.goldenSnitchPopped +
            '      }}' +
            '    }' +
            '  },' +
            '  { "insert" : {' +
            '      "object" : {"com.redhatkeynote.score.AchievementList":{' +
            '      }},' +
            '      "out-identifier" : "newAchievements",' +
            '      "return-object" : true' +
            '    }' +
            '  },' +
            '  {' +
            '    "fire-all-rules" : {}' +
            '  } ]' +
            '}'

    future.setHandler({ ar ->
      // We have finished the request.  If we cannot set the new default then a score has come in,
      // we process again
      def empty = player.aggregatedScore.compareAndSet(null, new AggregatedScore())
      if (!empty) {
        processSend(player)
      }
    })

    def clientRequest = scoreClient.post(scorePort, scoreHost, scorePath, { resp ->
      resp.exceptionHandler({ t ->
        t.printStackTrace();
        retryProcessSend(attempt, player, aggregatedScore, future)
      });
      if (resp.statusCode() == 200) {
        resp.bodyHandler { body ->
          def bodyContents = body.toString()
          Map response = Json.decodeValue(bodyContents, Map.class);
          if (response?.type == "SUCCESS") {
            def results = response?.result?.get('execution-results')?.results
            if (results) {
              for (result in results) {
                if (result.key == "newAchievements") {
                  def achievements = result.value.get('com.redhatkeynote.score.AchievementList')?.achievements
                  updateAchievements(player, achievements)
                  send(player, [
                          type        : 'achievements',
                          achievements: achievements
                  ])
                }
              }
            }
          } else {
            println("Unsuccessful, received the following body from the score server: " + bodyContents)
          }
          future.complete()
        }
      } else {
        println("Received error response from Score endpoint: " + resp.statusMessage());
        retryProcessSend(attempt, player, aggregatedScore, future)
      }
    })
            .putHeader('Accept', 'application/json')
            .putHeader('Content-Type', 'application/json')
            .setTimeout(10000)
            .exceptionHandler({ t ->
      t.printStackTrace()
      retryProcessSend(attempt, player, aggregatedScore, future)
    })

    if (scoreAuthHeader) {
      clientRequest.putHeader("Authorization", scoreAuthHeader)
    }

    clientRequest.end(playerUpdate)
  }

  // delete scores on score server
  def deleteScores() {
    Future future = Future.future()
    def deleteScores = '{' +
            '  "lookup"   : "ScoreSession",' +
            '  "commands" : [' +
            '  { "insert" : {' +
            '      "object" : {"com.redhatkeynote.score.DeletePlayers":{' +
            '      }}' +
            '    }' +
            '  },' +
            '  { "insert" : {' +
            '      "object" : {"com.redhatkeynote.score.AchievementList":{' +
            '      }},' +
            '      "out-identifier" : "newAchievements",' +
            '      "return-object" : true' +
            '    }' +
            '  },' +
            '  {' +
            '    "fire-all-rules" : {}' +
            '  } ]' +
            '}'

    def clientRequest = scoreClient.post(scorePort, scoreHost, scorePath, { resp ->
      resp.exceptionHandler({ t ->
        t.printStackTrace();
        future.complete()
      });
      if (resp.statusCode() != 200) {
        println("Received error response from Score endpoint while deleting: " + resp.statusMessage());
      }
      future.complete()
    })
            .putHeader('Accept', 'application/json')
            .putHeader('Content-Type', 'application/json')
            .setTimeout(10000)
            .exceptionHandler({ t ->
      t.printStackTrace()
      future.complete()
    })

    if (scoreAuthHeader) {
      clientRequest.putHeader("Authorization", scoreAuthHeader)
    }

    clientRequest.end(deleteScores)
    return future
  }

  def send(user, Map message) {
    vertx.eventBus().send(user.userId, message)
  }

  def send(user, List message) {
    vertx.eventBus().send(user.userId, message)
  }

  def publish(String address, Map message) {
    vertx.eventBus().publish(address, message)
  }

  def publish(String address, List message) {
    vertx.eventBus().publish(address, message)
  }


  private Handler<Message<List>> updateTeamScores() {
    { m ->
      List scores = m.body();
      def changed = false
      scores.each { score ->
        def team = teams.get(score.team)
        if (team.score != score.score) {
          team.score = score.score
          changed = true
        }
      }

// BURR - need to review
      if (changed || (state == 'play')) {
        broadcastTeamScores()
      }
    }
  }

  def updateAchievements(Player player, List achievements) {
    def currentAggregatedAchievements = player.aggregatedAchievements.get();
    if (currentAggregatedAchievements == null) {
      // If the new aggregate is null then a request is in progress
      def aggregatedAchievements = new AggregatedAchievements(achievements)
      if (!player.aggregatedAchievements.compareAndSet(currentAggregatedAchievements, aggregatedAchievements)) {
        // The state has changed so retry
        updateAchievements(player, achievements)
      }
    } else {
      def newAchievements = []
      newAchievements.addAll(achievements)
      newAchievements.addAll(currentAggregatedAchievements.achievements)
      def aggregatedAchievements = new AggregatedAchievements(newAchievements)
      if (!player.aggregatedAchievements.compareAndSet(currentAggregatedAchievements, aggregatedAchievements)) {
        // The state has changed so retry
        updateAchievements(player, achievements)
      } else if (currentAggregatedAchievements.isDefaulted()) {
        // If the previous aggregate has default scores then the previous request has finished so we restart
        processUpdateAchievements(player)
      }
    }
  }

  def processUpdateAchievements(Player player) {
    def future = Future.future()
    future.setHandler({ ar ->
      // We have finished the request.  If we cannot set the new default then a score has come in, we process again
      def empty = player.aggregatedAchievements.compareAndSet(null, new AggregatedAchievements())
      if (!empty) {
        processUpdateAchievements(player)
      }
    })

    def String uuid = player.userId
    def aggregatedAchievements = player.aggregatedAchievements.getAndSet(null)
    def achievements = aggregatedAchievements.achievements
    if (achievements && achievements.size() > 0) {

      def path = achievementPath + "/achievement/update/" + uuid;

      achievementClient.put(achievementPort, achievementHost, path, { resp ->
        // We don't read the body, no need for exception handler here.
        if (resp.statusCode() != 200) {
          println("Received error response from Achievement endpoint: " + resp.statusMessage());
        }
        future.complete()
      })
              .setTimeout(3000)
              .exceptionHandler({ t -> t.printStackTrace(); future.complete() })
              .putHeader("Content-Type", "application/json")
              .end(Json.encode(achievements));
    } else {
      future.complete()
    }
  }

  def resetAchievements() {
    def path = achievementPath + "/reset";

    Future future = Future.future()
    achievementClient.delete(achievementPort, achievementHost, path, { resp ->
      // We don't read the body, no need for exception handler here.
      future.complete()
      if (resp.statusCode() != 204) {
        println("Received error response from Achievement endpoint: " + resp.statusMessage());
      }
    })
            .setTimeout(3000)
            .exceptionHandler({ future.fail(t) })
            .putHeader("Content-Type", "application/json")
            .end();
    return future
  }

  def broadcastTeamScores() {
    teams.each { i, team ->
      def message = [
              type : 'team-score',
              score: team.score
      ]

      team.players.each { player ->
        send(player, message);
      }
    }
  }

  def setConfiguration(Map configuration) {
    teams.each { i, team ->
      team.configuration = configuration;
    }
    adminConfiguration = configuration;

    // println('Broadcasting configuration : ' + configuration);
    broadcastConfigurationChange();
  }

  def broadcastConfigurationChange() {
    teams.each { i, team ->
      def configurationMessage = [
              type         : 'configuration',
              configuration:
                      team.configuration
      ]

      broadcastTeamMessage(team, configurationMessage);
    }
    def configurationMessage = [
            type         : 'configuration',
            configuration: adminConfiguration
    ]
    broadcastAdminMessage(configurationMessage);
  }

  Handler<Message> onStateChange(EventBus eventBus) {
    { m ->
      def data = m.body();
      setState(data.state);
    }
  }

  def setState(_state) {
    state = _state;

    def message = [
            type : 'state',
            state: state
    ];

    broadcastAllMessage(message);
    broadcastAdminMessage(message);
  }

  Handler<Message> onSelfieStateChange(EventBus eventBus) {
    { m ->
      def data = m.body();
      setSelfieState(data.state);
    }
  }

  def setSelfieState(_state) {
    selfieState = _state;

    def message = [
            type : 'selfie-state',
            state: selfieState
    ];

    broadcastAllMessage(message);
    broadcastAdminMessage(message);
  }


  def broadcastAllMessage(Map message) {
    teams.each({ i, team -> broadcastTeamMessage(team, message) })
  }

  def broadcastTeamMessage(Team team, Map message) {
    def trafficPercentage = message.configuration?.trafficPercentage
    if (message.type == "configuration" && trafficPercentage && trafficPercentage != 100) {

      if (team.players.size() > 0) {
        long seed = System.nanoTime();
        Collections.shuffle(team.players, new Random(seed));

        def numPlayers = (int) Math.ceil(team.players.size() * (trafficPercentage / 100)) - 1;
        def playersSubset = [];
        team.players[0..numPlayers].each { playersSubset << it }

        playersSubset.each { player ->
          send(player, message);
        }
      }
    } else {
      team.players.each { player ->
        send(player, message)
      }
    }
  }

  def broadcastAdminMessage(Map message) {
    admins.each { admin ->
      send(admin, message)
    }
  }

  static def retrieveEndpoint(String env, int testPort, String testPath) {
    def endpoint = GameUtils.retrieveEndpoint(env, testPort, testPath);

    return [endpoint.host, endpoint.port, endpoint.path]
  }

  def getTeamCounter() {
    def future = Future.future()

    vertx.sharedData().getCounter(TEAM_COUNTER_NAME, { ar ->
      counter = ar.result()
      if (ar.succeeded()) {
        future.complete()
      } else {
        future.fail(ar.cause())
      }
    })
    return future
  }

  def getIndividualTeamCounters() {
    def futures = []
    for (team in teams.keySet()) {
      def future = Future.future()
      futures.add(future)
      def name = TEAM_COUNTER_NAME + "." + team;
      def teamNumber = team;
      vertx.sharedData().getCounter(name, { ar ->
        teamCounters[teamNumber] = ar.result()
        if (ar.succeeded()) {
          future.complete()
        } else {
          future.fail(ar.cause())
        }
      })
    }
    return futures
  }

  def getIndividualTeamPopCounters() {
    def futures = []
    for (team in teams.keySet()) {
      def future = Future.future()
      futures.add(future)
      def name = TEAM_POP_COUNTER_NAME + "." + team;
      def teamNumber = team;
      vertx.sharedData().getCounter(name, { ar ->

        teamPopCounters[teamNumber] = ar.result()
        if (ar.succeeded()) {
          future.complete()
        } else {
          future.fail(ar.cause())
        }
      })
    }
    return futures
  }

  def getPlayerNameMap() {
    def future = Future.future()

    vertx.sharedData().getClusterWideMap(PLAYER_NAME_MAP, { ar ->
      playerNames = ar.result()
      if (ar.succeeded()) {
        future.complete()
      } else {
        future.fail(ar.cause())
      }
    })
    return future
  }

  def resetAll() {
    def futures = []
    futures.add(deleteScores())
    futures.add(resetAchievements())
    teamPopCounters.each { k, v ->
      Future future = Future.future()
      futures.add(future)
      def currentCounter = v
      currentCounter.get({ nar ->
        currentCounter.addAndGet(-1 * nar.result(), { inner ->
          future.complete()
        });
      });
    }
    Future composite = Future.future()
    CompositeFuture.all(futures).setHandler({ ar ->
      if (ar.succeeded()) {
        composite.complete();
      } else {
        ar.cause().printStackTrace();
        composite.fail(ar.cause());
      }
    })
    return composite
  }
}
