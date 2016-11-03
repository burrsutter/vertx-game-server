class AggregatedAchievements {

  def defaulted
  def achievements

  AggregatedAchievements(achievements) {
    this.achievements = achievements
    this.defaulted = false
  }

  AggregatedAchievements() {
    this.achievements = []
    this.defaulted = true
  }

  Map toMap() {
    return [
            'achievements'      : achievements,
            'defaulted'         : defaulted
    ]
  }

  boolean isDefaulted() {
    return defaulted
  }

  String toString() {
    return 'achievements: ' + achievements + ', defaulted: ' + defaulted;
  }
}
