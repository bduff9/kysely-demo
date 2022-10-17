import { sql } from "kysely";

import { db, getTableRef } from "./db";

export const processWeeklyMV = async (week: number): Promise<void> => {
  await db.transaction().execute(async (trx) => {
    await sql.raw(`set @week := ${week}`).execute(trx);
    await sql`set foreign_key_checks = 0`.execute(trx);

    const weeklyMV = getTableRef("WeeklyMV");
    const systemValues = getTableRef("SystemValues");
    const picks = getTableRef("Picks");
    const games = getTableRef("Games");
    const users = getTableRef("Users");
    const tiebreakers = getTableRef("Tiebreakers");

    await sql`lock tables ${weeklyMV} write, ${weeklyMV} as W write, ${weeklyMV} as W2 write, ${systemValues} as SV read, ${picks} read, ${picks} as P read, ${games} read, ${games} as G read, ${games} as G2 read, ${games} as G3 read, ${games} as G4 read, ${users} read, ${users} as U read, ${tiebreakers} read, ${tiebreakers} as T read`
      .execute(trx);
    await trx.deleteFrom("WeeklyMV").where(
      "Week",
      "=",
      sql.raw("@week").castTo<number>(),
    ).execute();
    await trx.insertInto("WeeklyMV").columns([
      "Week",
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
      "TiebreakerScore",
      "LastScore",
      "TiebreakerIsUnder",
      "TiebreakerDiffAbsolute",
    ]).expression((qb) => {
      return qb.selectFrom("Picks as P").innerJoin(
        "Games as G",
        "G.GameID",
        "P.GameID",
      ).innerJoin("Users as U", "U.UserID", "P.UserID")
        .innerJoin(
          "Tiebreakers as T",
          (join) =>
            join.onRef("T.UserID", "=", "U.UserID").onRef(
              "T.TiebreakerWeek",
              "=",
              "G.GameWeek",
            ),
        )
        .select([
          (qb) => qb.fn.max("G.GameWeek").as("GameWeek"),
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
            const totalPointsSub = qb.selectFrom("Games as G2").select([
              sql<number>`(sum(1) * (sum(1) + 1)) DIV 2`.as("weekTotalPoints"),
            ]).whereRef("G2.GameWeek", "=", "G.GameWeek");
            const teamID = qb.ref("P.TeamID");
            const winnerID = qb.ref("G.WinnerTeamID");
            const points = qb.ref("P.PickPoints");

            return sql<
              number
            >`max(${totalPointsSub}) - sum(case when ${teamID} <> ${winnerID} and ${winnerID} is not null then ${points} else 0 end)`
              .as("PointsPossible");
          },
          (qb) => {
            const totalPointsSub = qb.selectFrom("Games as G3").select([
              sql<number>`(sum(1) * (sum(1) + 1)) DIV 2`.as("weekTotalPoints"),
            ]).whereRef("G3.GameWeek", "=", "G.GameWeek");

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
          (qb) => {
            const lastScore = qb.ref("T.TiebreakerLastScore");

            return sql<number>`max(${lastScore})`.as("TiebreakerScore");
          },
          (qb) => {
            const lastScoreSub = qb.selectFrom("Games as G4").select([
              (qb) => {
                const gameStatus = qb.ref("G4.GameStatus");
                const homeScore = qb.ref("G4.GameHomeScore");
                const visitorScore = qb.ref("G4.GameVisitorScore");

                return sql<
                  number
                >`case when ${gameStatus} = 'Final' then ${homeScore} + ${visitorScore} else null end`
                  .as("lastScore");
              },
            ]).whereRef("G4.GameWeek", "=", "G.GameWeek").orderBy(
              "G4.GameKickoff",
              "desc",
            ).limit(1);

            return sql<number>`max(${lastScoreSub})`.as("LastScore");
          },
          sql.literal(true).as("TiebreakerDiff"),
          sql.literal(0).as("TiebreakerDiffAbsolute"),
        ]).where("G.GameWeek", "=", sql.raw("@week").castTo<number>()).groupBy(
          "U.UserID",
        ).orderBy(
          "PointsEarned",
          "desc",
        ).orderBy("GamesCorrect", "desc");
    }).execute();
    await trx.updateTable("WeeklyMV").set({
      TiebreakerIsUnder: sql<number>`TiebreakerScore <= LastScore`,
      TiebreakerDiffAbsolute: sql<number>`abs(TiebreakerScore - LastScore)`,
    }).where("Week", "=", sql.raw("@week").castTo<number>()).where(
      "LastScore",
      "is not",
      null,
    ).execute();
    await sql`select @curRank := 0, @prevPoints := null, @prevGames := null, @prevTiebreaker := null, @playerNumber := 1`
      .execute(trx);
    await trx.insertInto("WeeklyMV").columns([
      "Week",
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
      "TiebreakerScore",
      "LastScore",
      "TiebreakerIsUnder",
      "TiebreakerDiffAbsolute",
    ]).expression((qb) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("WeeklyMV as W").select([
          "W.Week",
          "W.UserID",
          "W.TeamName",
          "W.UserName",
          "W.PointsEarned",
          "W.PointsWrong",
          "W.PointsPossible",
          "W.PointsTotal",
          "W.GamesCorrect",
          "W.GamesWrong",
          "W.GamesPossible",
          "W.GamesTotal",
          "W.GamesMissed",
          "W.TiebreakerScore",
          "W.LastScore",
          "W.TiebreakerIsUnder",
          "W.TiebreakerDiffAbsolute",
          (qb) => {
            const pointsEarned = qb.ref("W.PointsEarned");
            const gamesCorrect = qb.ref("W.GamesCorrect");
            const tiebreaker = qb.ref("W.TiebreakerScore");
            const lastScore = qb.ref("W.LastScore");

            return sql<
              number
            >`@curRank := if(@prevPoints = ${pointsEarned} and @prevGames = ${gamesCorrect} and (@prevTiebreaker = ${tiebreaker} or ${lastScore} is null), @curRank, @playerNumber)`
              .as("Rank");
          },
          sql<number>`@playerNumber := @playerNumber + 1`.as("playerNumber"),
          (qb) => {
            const pointsEarned = qb.ref("W.PointsEarned");

            return sql<number>`@prevPoints := ${pointsEarned}`.as("prevPoints");
          },
          (qb) => {
            const gamesCorrect = qb.ref("W.GamesCorrect");

            return sql<number>`@prevGames := ${gamesCorrect}`.as("prevGames");
          },
          (qb) => {
            const tiebreaker = qb.ref("W.TiebreakerScore");

            return sql<number>`@prevTiebreaker := ${tiebreaker}`.as(
              "prevTiebreaker",
            );
          },
        ]).where("W.Week", "=", sql.raw("@week").castTo<number>()).orderBy(
          "PointsEarned",
          "desc",
        ).orderBy("GamesCorrect", "desc").orderBy("W.TiebreakerIsUnder", "desc")
          .orderBy("W.TiebreakerDiffAbsolute", "asc").as("f")
      ).select([
        "Week",
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
        "TiebreakerScore",
        "LastScore",
        "TiebreakerIsUnder",
        "TiebreakerDiffAbsolute",
      ])
    ).execute();

    await trx.deleteFrom("WeeklyMV").where("Rank", "=", 0).where(
      "Week",
      "=",
      sql.raw("@week").castTo<number>(),
    ).execute();
    //FIXME: UPDATEs with JOINs not currently work in Kysely, https://github.com/koskimas/kysely/issues/192
    await trx.updateTable("WeeklyMV as W").set({ Tied: 1 }).whereExists((qb) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("WeeklyMV as W2")
          .select([sql.literal(1).as("Found")])
          .whereRef("W.Rank", "=", "W2.Rank")
          .whereRef("W.UserID", "<>", "W2.UserID")
          .whereRef("W.Week", "=", "W2.Week").as("INNER")
      ).select("INNER.Found")
    ).where("W.Week", "=", sql.raw("@week").castTo<number>()).execute();
    //FIXME: UPDATEs with JOINs not currently work in Kysely, https://github.com/koskimas/kysely/issues/192
    await trx.updateTable("WeeklyMV as W").set({ IsEliminated: 1 })
      .whereExists((qb) =>
        qb.selectFrom((qb) =>
          qb.selectFrom("WeeklyMV as W2").select([sql.literal(1).as("Found")])
            .whereRef("W2.PointsEarned", ">", "W.PointsPossible")
            .whereRef("W.Week", "=", "W2.Week").as("INNER")
        ).select("INNER.Found")
      ).where("W.Week", "=", sql.raw("@week").castTo<number>()).execute();
    await sql`unlock tables`.execute(trx);
    await sql`set foreign_key_checks = 1`.execute(trx);
  });
};
