class Admin {
  def userId = UUID.randomUUID().toString()
  def ping = -1

  Map toMap() {
    return [
            'userId': userId
    ]
  }
}