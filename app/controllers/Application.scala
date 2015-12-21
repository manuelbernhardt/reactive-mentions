package controllers

import java.sql.Timestamp
import javax.inject.Inject

import database.DB
import org.joda.time.DateTime
import play.api._
import play.api.mvc._

class Application @Inject() (db: DB) extends Controller {

  def index = Action.async { implicit request =>

    import generated.Tables._
    import org.jooq.impl.DSL._

    db.query { sql =>
      val mentionsCount = sql.select(
        count()
      ).from(MENTIONS)
       .where(
         MENTIONS.CREATED_ON.gt(value(new Timestamp(DateTime.now.minusDays(1).getMillis)))
       ).execute()

      Ok(views.html.index(mentionsCount))
    }

  }

}
