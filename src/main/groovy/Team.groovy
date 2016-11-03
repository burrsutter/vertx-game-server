class Team {

  def number
  def score = 0
  List<Player> players = []
  def configuration = [:]

  Team(number) {
    this.number = number
  }

  Map toMap() {
    return [
            'number'       : number,
            'score'        : score,
            'players'      : players,
            'configuration': configuration
    ]
  }

  static def createTeams(int numTeams) {
    def teams = [:]
    for (def i = 1; i < numTeams + 1; i++) {
      def team = new Team(i);
      teams.put(i, team);
    }
    return teams;
  }
}