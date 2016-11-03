import com.redhat.middleware.keynote.UserNameGenerator
import java.util.concurrent.atomic.AtomicReference


class Player {
  def userId
  def team
  def username
  AtomicReference aggregatedScore = new AtomicReference(new AggregatedScore());
  AtomicReference aggregatedAchievements = new AtomicReference(new AggregatedAchievements());

  Player(userId, team, username) {
    this.userId = userId
    this.team = team
    this.username = username
  }

  Player(team) {
    this(UUID.randomUUID().toString(), team, UserNameGenerator.generate())
  }

  Map toMap() {
    return [
            'userId': userId,
            'team' : team,
            'username' : username
    ]
  }
}
