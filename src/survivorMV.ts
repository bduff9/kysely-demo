import { sql } from "kysely";

import { db, getTableRef } from "./db";

export const processSurvivorMV = async (week: number): Promise<void> => {
  await db.transaction().execute(async (trx) => {
    await sql.raw(`set @week := ${week}`).execute(trx);
    await sql`set foreign_key_checks = 0`.execute(trx);

    const survivorMV = getTableRef("SurvivorMV");
    const survivorPicks = getTableRef("SurvivorPicks");
    const games = getTableRef("Games");
    const users = getTableRef("Users");

    await sql`lock tables ${survivorMV} write, ${survivorMV} as S1 write, ${survivorMV} as S2 write, ${games} read, ${games} as G read, ${survivorPicks} read, ${survivorPicks} as S read, ${survivorPicks} as SP read, ${survivorPicks} as SP2 read, ${users} read, ${users} as U read`
      .execute(trx);
    await trx.deleteFrom("SurvivorMV").execute();
    await trx.insertInto("SurvivorMV").columns([
      "Rank",
      "Tied",
      "UserID",
      "UserName",
      "TeamName",
      "WeeksAlive",
      "IsAliveOverall",
      "CurrentStatus",
      "LastPick",
    ]).expression((qb) => {
      return qb.selectFrom("SurvivorPicks as S").innerJoin(
        "Games as G",
        "G.GameID",
        "S.GameID",
      ).innerJoin("Users as U", "U.UserID", "S.UserID").select([
        sql.literal(0).as("Rank"),
        sql.literal(false).as("Tied"),
        "U.UserID",
        "U.UserName",
        (qb) => {
          const teamName = qb.ref("U.UserTeamName");
          const firstName = qb.ref("U.UserFirstName");

          return sql<
            string
          >`case when ${teamName} is null or ${teamName} = '' then concat(${firstName}, '''s team') else ${teamName} end`
            .as("TeamName");
        },
        (qb) =>
          qb.selectFrom("SurvivorPicks as SP").select([
            (qb) => qb.fn.count<number>("SP.SurvivorPickWeek").as("count"),
          ]).whereRef("SP.UserID", "=", "S.UserID").where(
            "SP.SurvivorPickDeleted",
            "is",
            null,
          ).where(
            "SP.SurvivorPickWeek",
            "<=",
            sql.raw("@week").castTo<number>(),
          ).as("WeeksAlive"),
        (qb) => {
          const deleted = qb.ref("S.SurvivorPickDeleted");
          const teamID = qb.ref("S.TeamID");
          const winnerID = qb.ref("G.WinnerTeamID");

          return sql<
            number
          >`case when ${deleted} is not null then false when ${teamID} is null then false when ${teamID} = ${winnerID} or ${winnerID} is null then true else false end`
            .as("IsAliveOverall");
        },
        (qb) => {
          const deleted = qb.ref("S.SurvivorPickDeleted");
          const teamID = qb.ref("S.TeamID");
          const winnerID = qb.ref("G.WinnerTeamID");

          return sql<
            number
          >`case when ${deleted} is not null then null when ${teamID} is null then 'Dead' when ${winnerID} is null then 'Waiting' when ${teamID} = ${winnerID} then 'Alive' else 'Dead' end`
            .as("CurrentStatus");
        },
        (qb) =>
          qb.selectFrom("SurvivorPicks as SP2").select(["SP2.TeamID"]).whereRef(
            "SP2.UserID",
            "=",
            "S.UserID",
          ).where(
            "SP2.SurvivorPickWeek",
            "<=",
            sql.raw("@week").castTo<number>(),
          ).where("SP2.SurvivorPickDeleted", "is", null).orderBy(
            "SP2.SurvivorPickWeek",
            "desc",
          ).limit(1).as("LastPick"),
      ]).where("S.SurvivorPickWeek", "=", sql.raw("@week").castTo<number>())
        .orderBy(
          "IsAliveOverall",
          "desc",
        ).orderBy("WeeksAlive", "desc");
    }).execute();

    await sql`select @curRank := 0, @prevIsAlive := null, @prevWeeksAlive := null, @playerNumber := 1`
      .execute(trx);
    await trx.insertInto("SurvivorMV").columns([
      "Rank",
      "Tied",
      "UserID",
      "TeamName",
      "UserName",
      "WeeksAlive",
      "IsAliveOverall",
      "CurrentStatus",
      "LastPick",
    ]).expression((qb) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("SurvivorMV as S1").select([
          "S1.UserID",
          "S1.TeamName",
          "S1.UserName",
          "S1.WeeksAlive",
          "S1.IsAliveOverall",
          "S1.CurrentStatus",
          "S1.LastPick",
          (qb) => {
            const isAlive = qb.ref("S1.IsAliveOverall");
            const weeksAlive = qb.ref("S1.WeeksAlive");

            return sql<
              number
            >`@curRank := if(@prevIsAlive = ${isAlive} and @prevWeeksAlive = ${weeksAlive}, @curRank, @playerNumber)`
              .as("Rank");
          },
          sql<number>`@playerNumber := @playerNumber + 1`.as("playerNumber"),
          sql<number>`@prevIsAlive := isAliveOverall`.as("prevIsAlive"),
          sql<number>`@prevWeeksAlive := WeeksAlive`.as("prevWeeksAlive"),
        ]).orderBy("IsAliveOverall", "desc").orderBy("WeeksAlive", "desc").as(
          "f",
        )
      ).select([
        "Rank",
        sql.literal(false).as("Tied"),
        "UserID",
        "TeamName",
        "UserName",
        "WeeksAlive",
        "IsAliveOverall",
        "CurrentStatus",
        "LastPick",
      ])
    ).execute();

    await trx.deleteFrom("SurvivorMV").where("Rank", "=", 0).execute();
    //FIXME: UPDATEs with JOINs not currently work in Kysely, https://github.com/koskimas/kysely/issues/192
    await trx.updateTable("SurvivorMV as S1").set({ Tied: 1 }).whereExists((
      qb,
    ) =>
      qb.selectFrom((qb) =>
        qb.selectFrom("SurvivorMV as S2")
          .select([sql.literal(1).as("Found")])
          .whereRef("S1.Rank", "=", "S2.Rank")
          .whereRef("S1.UserID", "<>", "S2.UserID").as("INNER")
      ).select("INNER.Found")
    ).execute();
    await sql`unlock tables`.execute(trx);
    await sql`set foreign_key_checks = 1`.execute(trx);
  });
};
