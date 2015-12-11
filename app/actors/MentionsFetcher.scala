package actors

import java.util.Locale

import actors.MentionsFetcher._
import akka.actor.{ActorLogging, Actor}
import org.joda.time.DateTime
import scala.concurrent.duration._

import play.api.Play
import play.api.Play.current
import play.api.libs.oauth.{OAuthCalculator, RequestToken, ConsumerKey}
import akka.pattern.pipe

import scala.util.control.NonFatal
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.JsArray
import play.api.libs.ws.WS
import scala.concurrent.Future


class MentionsFetcher extends Actor with ActorLogging {

  implicit val executionContext = context.dispatcher

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

  var lastSeenMentionTime: Option[DateTime] = Some(DateTime.now)

  def checkMentions = {
      val maybeMentions = for {
        (consumerKey, requestToken) <- credentials
        time <- lastSeenMentionTime
      } yield fetchMentions(consumerKey, requestToken, "elmanu", time)

      maybeMentions.foreach { mentions =>
        mentions.map { m =>
          MentionsReceived(m)
        } recover { case NonFatal(t) =>
          log.error(t, "Could not fetch mentions")
          MentionsReceived(Seq.empty)
        } pipeTo self
      }
  }

  def fetchMentions(consumerKey: ConsumerKey, requestToken: RequestToken, user: String, time: DateTime): Future[Seq[Mention]] = {
    val df = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH)

    WS.url("https://api.twitter.com/1.1/search/tweets.json")
      .sign(OAuthCalculator(consumerKey, requestToken))
      .withQueryString("q" -> s"@$user")
      .get()
      .map { response =>
        val mentions = (response.json \ "statuses").as[JsArray].value.map { status =>
          val id = (status \ "id_str").as[String]
          val text = (status \ "text").as[String]
          val from = (status \ "user" \ "screen_name").as[String]
          val created_at = df.parseDateTime((status \ "created_at").as[String])
          val userMentions = (status \ "entities" \ "user_mentions").as[JsArray].value.map { user =>
            User((user \ "screen_name").as[String], ((user \ "id_str").as[String]))
          }

          Mention(id, created_at, from, text, userMentions)
        }
        mentions.filter(_.created_at.isAfter(time))
    }
  }

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