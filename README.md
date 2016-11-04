# Vert.x Game Server

This project is an implementation of the Game server on top of Vert.x.

It provides the HTTP endpoints and websockets for the _game_ part of the demo.

## Structure

The projects is composed by 3 verticles:

* `MainVerticle` just deploys the 2 other verticles
* `ServerVerticle` creates a HTTP server and manage HTTP requests and web sockets, data is computed by the `GameVerticle`
* `GameVerticle` manages the game. It is written in Groovy
 
This architecture lets us increase the number of instances of the `GameVerticle` to face the load. The `ServerVerticle` and `GameVerticle` are communicating using the event bus. So, potentially, the `GameVerticle` instances can be located on different computers.  

## Build and Run

```
mvn clean package -DskipTests
```

It generates a _fat jar_ in `target`:  ` game-service-1.0.0-SNAPSHOT-fat.jar`

Launch it with: `java -jar target/game-service-1.0.0-SNAPSHOT-fat.jar - cluster`

Tests are simulating a couple of loads, so may takes a bit of time to run.

## Fast run for dev

```
mvn compile exec:exec@run
```

## Redeploy for Dev

```
vertx run src/main/java/com/redhat/middleware/keynote/MainVerticle.java -cluster -cp target/game-service.jar --redeploy=src/main/**/* --launcher-class=io.vertx.core.Launcher --on-redeploy="mvn compile package -DskipTests"
```

## Configuration

You can pass a configuration object using: `-conf my-configuration.json`.

This configuration can contain:

* `game-verticle-instances` : the number of `GameVerticle` instances, 1 by default, can be 0
* `server-verticle-instances` : the number of `ServerVerticle` instances, 4 by default, can be 0
* `port`: the HTTP server port, `9001` by default
* `number-of-teams`: the number of teams, 4 by default
* `score-broadcast-interval`: the period in ms between 2 score broadcast, 2500 by default

export ACHIEVEMENTS_SERVER=localhost
export ACHIEVEMENTS_SERVER_PORT=9090
export SCORE_SERVER=localhost
export SCORE_SERVER_PORT=8080
export SCORE_USER=kiewb
export SCORE_PASSWORD=kiewb