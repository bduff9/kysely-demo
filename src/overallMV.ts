import { sql } from "kysely";

import { db, getTableRef } from "./db";

export const processOverallMV = async (
  week: number,
  useNewMethod = false,
): Promise<void> => {
  await db.transaction().execute(async (trx) => {
    await sql.raw(`set @week := ${week}`).execute(trx);
    await sql`set foreign_key_checks = 0`.execute(trx);

    const overallMV = getTableRef("OverallMV");
    const weeklyMV = getTableRef("WeeklyMV");
    const systemValues = getTableRef("SystemValues");
    const picks = getTableRef("Picks");
    const games = getTableRef("Games");
    const users = getTableRef("Users");

    await sql`lock tables ${overallMV} write, ${overallMV} as O write, ${overallMV} as O2 write, ${weeklyMV} as W read, ${systemValues} as SV read, ${picks} read, ${picks} as P read, ${games} read, ${games} as G read, ${games} as G2 read, ${games} as G3 read, ${users} read, ${users} as U read`
      .execute(trx);
    await trx.deleteFrom("OverallMV").execute();
    if (useNewMethod) {
      await trx.insertInto("OverallMV").columns([
        "Rank",
        "Tied",
        "UserID",
        "TeamName",
        "UserName",
        "PointsEarned",
        "PointsWrong",
        "PointsPossible",
        "PointsTotal",
        "GamesCorrect",
        "GamesWrong",
        "GamesPossible",
        "GamesTotal",
        "GamesMissed",
      ]).expression((qb) =>
        qb.selectFrom("WeeklyMV as W").select([
          sql.literal(0).as("Rank"),
          sql.literal(false).as("Tied"),
          "W.UserID",
          (qb) => qb.fn.max("W.TeamName").as("TeamName"),
          (qb) => qb.fn.max("W.UserName").as("UserName"),
          (qb) => qb.fn.sum("W.PointsEarned").as("PointsEarned"),
          (qb) => qb.fn.sum("W.PointsWrong").as("PointsWrong"),
          (qb) => qb.fn.sum("W.PointsPossible").as("PointsPossible"),
          (qb) => qb.fn.sum("W.PointsTotal").as("PointsTotal"),
          (qb) => qb.fn.sum("W.GamesCorrect").as("GamesCorrect"),
          (qb) => qb.fn.sum("W.GamesWrong").as("GamesWrong"),
          (qb) => qb.fn.sum("W.GamesPossible").as("GamesPossible"),
          (qb) => qb.fn.sum("W.GamesTotal").as("GamesTotal"),
          (qb) => qb.fn.sum("W.GamesMissed").as("GamesMissed"),
        ]).groupBy("W.UserID").orderBy(
          "PointsEarned",
          "desc",
        ).orderBy("GamesCorrect", "desc")
      ).execute();
    } else {
      await trx.insertInto("OverallMV").columns([
        "Rank",
        "Tied",
        "UserID",
        "TeamName",
        "UserName",
        "PointsEarned",
        "PointsWrong",
        "PointsPossible",
        "PointsTotal",
        "GamesCorrect",
        "GamesWrong",
        "GamesPossible",
        "GamesTotal",
        "GamesMissed",
      ]).expression((qb) => {
        return qb.selectFrom("Picks as P").innerJoin(
          "Games as G",
          "G.GameID",
          "P.GameID",
        ).innerJoin("Users as U", "U.UserID", "P.UserID").select([
          sql.literal(0).as("Rank"),
          sql.literal(false).as("Tied"),
          "U.UserID",
          (qb) => {
            const teamName = qb.ref("U.UserTeamName");
            const firstName = qb.ref("U.UserFirstName");

            return sql<
              string
            >`case when ${teamName} is null or ${teamName} = '' then concat(${firstName}, '''s team') else ${teamName} end`
              .as("TeamName");
          },
          "U.UserName",
          (qb) => {
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");
            const points = qb.ref("P.PickPoints");

            return sql<
              number
            >`sum(case when ${teamID} = ${winnerID} then ${points} else 0 end)`
              .as(
                "PointsEarned",
              );
          },
          (qb) => {
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");
            const points = qb.ref("P.PickPoints");

            return sql<
              number
            >`sum(case when ${teamID} <> ${winnerID} and ${winnerID} is not null then ${points} else 0 end)`
              .as("PointsWrong");
          },
          (qb) => {
            const totalPointsPerWeekSub = qb.selectFrom("Games as G2").select([
              "G2.GameWeek",
              sql<number>`(sum(1) * (sum(1) + 1)) DIV 2`.as("weekTotalPoints"),
            ]).groupBy("G2.GameWeek").as("i");
            const totalPointsSub = qb.selectFrom(totalPointsPerWeekSub).select([
              (qb) => qb.fn.sum("i.weekTotalPoints").as("totalPoints"),
            ]).where("i.GameWeek", "<=", sql.raw("@week").castTo<number>());
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");
            const points = qb.ref("P.PickPoints");

            return sql<
              number
            >`max(${totalPointsSub}) - sum(case when ${teamID} <> ${winnerID} and ${winnerID} is not null then ${points} else 0 end)`
              .as("PointsPossible");
          },
          (qb) => {
            const totalPointsPerWeekSub = qb.selectFrom("Games as G3").select([
              "G3.GameWeek",
              sql<number>`(sum(1) * (sum(1) + 1)) DIV 2`.as("weekTotalPoints"),
            ]).groupBy("G3.GameWeek").as("i");
            const totalPointsSub = qb.selectFrom(totalPointsPerWeekSub).select([
              (qb) => qb.fn.sum("i.weekTotalPoints").as("totalPoints"),
            ]).where("i.GameWeek", "<=", sql.raw("@week").castTo<number>());

            return sql<
              number
            >`max(${totalPointsSub})`
              .as("PointsTotal");
          },
          (qb) => {
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");

            return sql<
              number
            >`sum(case when ${teamID} = ${winnerID} then 1 else 0 end)`.as(
              "GamesCorrect",
            );
          },
          (qb) => {
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");

            return sql<
              number
            >`sum(case when ${teamID} <> ${winnerID} and ${winnerID} is not null then 1 else 0 end)`
              .as("GamesWrong");
          },
          (qb) => {
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");

            return sql<
              number
            >`sum(case when ${teamID} = ${winnerID} or ${winnerID} is null then 1 else 0 end)`
              .as("GamesPossible");
          },
          sql<number>`sum(1)`.as("GamesTotal"),
          (qb) => {
            const weekDueSub = qb.selectFrom("SystemValues as SV").select([
              "SV.SystemValueValue",
            ]).where("SV.SystemValueName", "=", "PaymentDueWeek");
            const gameWeek = qb.ref("G.GameWeek");
            const teamID = qb.ref("P.TeamID");

            return sql<
              number
            >`sum(case when ${gameWeek} > ${weekDueSub} and ${teamID} is null then 1 else 0 end)`
              .as("GamesMissed");
          },
        ]).where("G.GameWeek", "<=", sql.raw("@week").castTo<number>()).groupBy(
          "U.UserID",
        ).orderBy(
          "PointsEarned",
          "desc",
        ).orderBy("GamesCorrect", "desc");
      }).execute();
    }
    await sql`select @curRank := 0, @prevPoints := null, @prevGames := null, @playerNumber := 1`
      .execute(trx);
    await trx.insertInto("OverallMV").columns([
      "Rank",
      "Tied",
      "UserID",
      "TeamName",
      "UserName",
      "PointsEarned",
      "PointsWrong",
      "PointsPossible",
      "PointsTotal",
      "GamesCorrect",
      "GamesWrong",
      "GamesPossible",
      "GamesTotal",
      "GamesMissed",
    ]).expression((qb) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("OverallMV as O").select([
          "O.UserID",
          "O.TeamName",
          "O.UserName",
          "O.PointsEarned",
          "O.PointsWrong",
          "O.PointsPossible",
          "O.PointsTotal",
          "O.GamesCorrect",
          "O.GamesWrong",
          "O.GamesPossible",
          "O.GamesTotal",
          "O.GamesMissed",
          (qb) => {
            const pointsEarned = qb.ref("O.PointsEarned");
            const gamesCorrect = qb.ref("O.GamesCorrect");

            return sql<
              number
            >`@curRank := if(@prevPoints = ${pointsEarned} and @prevGames = ${gamesCorrect}, @curRank, @playerNumber)`
              .as("Rank");
          },
          sql<number>`@playerNumber := @playerNumber + 1`.as("playerNumber"),
          (qb) => {
            const pointsEarned = qb.ref("O.PointsEarned");

            return sql<number>`@prevPoints := ${pointsEarned}`.as("prevPoints");
          },
          (qb) => {
            const gamesCorrect = qb.ref("O.GamesCorrect");

            return sql<number>`@prevGames := ${gamesCorrect}`.as("prevGames");
          },
        ]).orderBy("PointsEarned", "desc").orderBy("GamesCorrect", "desc").as(
          "f",
        )
      ).select([
        "Rank",
        sql.literal(false).as("Tied"),
        "UserID",
        "TeamName",
        "UserName",
        "PointsEarned",
        "PointsWrong",
        "PointsPossible",
        "PointsTotal",
        "GamesCorrect",
        "GamesWrong",
        "GamesPossible",
        "GamesTotal",
        "GamesMissed",
      ])
    ).execute();

    await trx.deleteFrom("OverallMV").where("Rank", "=", 0).execute();
    //FIXME: UPDATEs with JOINs not currently work in Kysely, https://github.com/koskimas/kysely/issues/192
    await trx.updateTable("OverallMV as O").set({ Tied: 1 }).whereExists((qb) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("OverallMV as O2")
          .select([sql.literal(1).as("Found")])
          .whereRef("O.Rank", "=", "O2.Rank")
          .whereRef("O.UserID", "<>", "O2.UserID").as("INNER")
      ).select("INNER.Found")
    ).execute();
    //FIXME: UPDATEs with JOINs not currently work in Kysely, https://github.com/koskimas/kysely/issues/192
    const totalPointsPerWeekSub = trx.selectFrom("Games as G2").select([
      "G2.GameWeek",
      sql<number>`(sum(1) * (sum(1) + 1)) DIV 2`.as("weekTotalPoints"),
    ]).groupBy("G2.GameWeek").as("i");
    const totalPointsResult = await trx.selectFrom(totalPointsPerWeekSub)
      .select([
        (qb) => qb.fn.sum("i.weekTotalPoints").as("totalPoints"),
      ]).executeTakeFirstOrThrow();
    const totalPoints = totalPointsResult.totalPoints;

    await trx.updateTable("OverallMV as O").set({ IsEliminated: 1 })
      .whereExists((qb) =>
        qb.selectFrom((qb) =>
          qb.selectFrom("OverallMV as O2").select([sql.literal(1).as("Found")])
            .where(
              "O2.PointsEarned",
              ">",
              (qb) =>
                qb.selectFrom("Picks as P").innerJoin(
                  "Games as G",
                  "G.GameID",
                  "P.GameID",
                ).select([
                  (qb) => {
                    const teamID = qb.ref("P.TeamID");
                    const winnerID = qb.ref("G.WinnerTeamID");
                    const points = qb.ref("P.PickPoints");

                    return sql`${totalPoints} - sum(case when ${teamID} <> ${winnerID} and ${winnerID} is not null then ${points} else 0 end)`
                      .as("SeasonPossiblePoints");
                  },
                ]).whereRef("P.UserID", "=", "O.UserID"),
            ).as("INNER")
        ).select("INNER.Found")
      ).execute();
    await sql`unlock tables`.execute(trx);
    await sql`set foreign_key_checks = 1`.execute(trx);
  });
};
