package actors

import java.sql.Timestamp
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

import javax.inject.Inject
import com.google.inject.AbstractModule
import play.api.libs.concurrent.AkkaGuiceSupport
import database.DB

import generated.Tables._
import org.jooq.impl.DSL._

class MentionsFetcher @Inject() (db: DB) extends Actor with ActorLogging {

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

  var lastSeenMentionTime: Option[DateTime] = Some(DateTime.now.minusDays(2))

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
            User((user \ "screen_name").as[String], (user \ "id_str").as[String])
          }

          Mention(id, created_at, text, from, userMentions)
        }
        mentions.filter(_.created_at.isAfter(time))
    }
  }

  def storeMentions(mentions: Seq[Mention]) = db.withTransaction { sql =>
    log.info("Inserting potentially {} mentions into the database", mentions.size)
    val now = new Timestamp(DateTime.now.getMillis)

    def upsertUser(handle: String) = {
      sql.insertInto(TWITTER_USER, TWITTER_USER.CREATED_ON, TWITTER_USER.TWITTER_USER_NAME)
        .select(
          select(value(now), value(handle))
            .whereNotExists(
              selectOne()
                .from(TWITTER_USER)
                .where(TWITTER_USER.TWITTER_USER_NAME.equal(handle))
            )
        )
        .execute()
    }

    mentions.foreach { mention =>
      // upsert the mentioning users
      upsertUser(mention.from)

      // upsert the mentioned users
      mention.users.foreach { user =>
        upsertUser(user.handle)
      }

      // upsert the mention
      sql.insertInto(MENTIONS, MENTIONS.CREATED_ON, MENTIONS.TEXT, MENTIONS.TWEET_ID, MENTIONS.USER_ID)
        .select(
          select(
            value(now),
            value(mention.text),
            value(mention.id),
            TWITTER_USER.ID
          )
            .from(TWITTER_USER)
            .where(TWITTER_USER.TWITTER_USER_NAME.equal(mention.from))
            .andNotExists(
              selectOne()
                .from(MENTIONS)
                .where(MENTIONS.TWEET_ID.equal(mention.id))
            )
        )
        .execute()
    }
  }

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

class MentionsFetcherModule extends AbstractModule with AkkaGuiceSupport {
  def configure(): Unit =
    bindActor[MentionsFetcher]("fetcher")
}
