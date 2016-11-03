class AggregatedScore {

  def defaulted
  def score
  def consecutivePops
  def goldenSnitchPopped

  AggregatedScore(score, consecutivePops, goldenSnitchPopped) {
    this.score = score
    this.consecutivePops = consecutivePops
    this.goldenSnitchPopped = goldenSnitchPopped
    this.defaulted = false
  }

  AggregatedScore() {
    this.score = 0
    this.consecutivePops = 0
    this.goldenSnitchPopped = false
    this.defaulted = true
  }

  Map toMap() {
    return [
            'score'             : score,
            'consecutivePops'   : consecutivePops,
            'goldenSnitchPopped': goldenSnitchPopped,
            'defaulted'         : defaulted
    ]
  }

  boolean isDefaulted() {
    return defaulted
  }

  String toString() {
    return 'score: ' + score + ', consecutivePops: ' + consecutivePops + ', goldenSnitchPopped: ' + goldenSnitchPopped + ', defaulted: ' + defaulted;
  }
}
