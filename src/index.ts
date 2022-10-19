import { processOverallMV } from "./overallMV";
import { processSurvivorMV } from "./survivorMV";
import { processWeeklyMV } from "./weeklyMV";

const main = async (): Promise<void> => {
  const week = 4;

  await processWeeklyMV(week);
  await processOverallMV(week, true);
  await processSurvivorMV(week);

  process.exit(0);
};

main();
