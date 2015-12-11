package actors

import actors.MentionsFetcher._
import akka.actor.{ActorLogging, Actor}
import org.joda.time.DateTime
import scala.concurrent.duration._

import play.api.Play
import play.api.Play.current
import play.api.libs.oauth.{RequestToken, ConsumerKey}

class MentionsFetcher extends Actor with ActorLogging {

  val scheduler = context.system.scheduler.schedule(
    initialDelay = 5.seconds,
    interval = 10.minutes,
    receiver = self,
    message = CheckMentions
  )

  override def postStop(): Unit = {
    scheduler.cancel()
  }

  def receive = {
    case CheckMentions => checkMentions
    case MentionsReceived(mentions) => storeMentions(mentions)
  }

  def checkMentions = ???

  def storeMentions(mentions: Seq[Mention]) = ???

  def credentials = for {
    apiKey <- Play.configuration.getString("twitter.apiKey")
    apiSecret <- Play.configuration.getString("twitter.apiSecret")
    token <- Play.configuration.getString("twitter.accessToken")
    tokenSecret <- Play.configuration.getString("twitter.accessTokenSecret")
  } yield (ConsumerKey(apiKey, apiSecret), RequestToken(token, tokenSecret))


}

object MentionsFetcher {

  case object CheckMentions
  case class Mention(id: String, created_at: DateTime, text: String, from: String, users: Seq[User])
  case class User(handle: String, id: String)
  case class MentionsReceived(mentions: Seq[Mention])

}