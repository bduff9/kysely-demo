import { processOverallMV } from "./overallMV";
import { processSurvivorMV } from "./survivorMV";
import { processWeeklyMV } from "./weeklyMV";

const main = async (): Promise<void> => {
  const week = 4;

  await processOverallMV(week);
  await processWeeklyMV(week);
  await processSurvivorMV(week);

  process.exit(0);
};

main();
