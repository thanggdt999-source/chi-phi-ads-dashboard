import { PrismaClient } from "@prisma/client";
import bcrypt from "bcryptjs";

const prisma = new PrismaClient();

async function main() {
  const adminPassword = await bcrypt.hash("Admin@Hexi2026!", 12);
  const leadPassword = await bcrypt.hash("LeadTeam1@2026", 12);
  const empPassword = await bcrypt.hash("Emp@123456", 12);

  // Admin
  await prisma.user.upsert({
    where: { username: "admin_root" },
    update: {},
    create: {
      username: "admin_root",
      password: adminPassword,
      displayName: "System Admin",
      role: "admin",
    },
  });

  // Leaders
  for (let i = 1; i <= 5; i++) {
    const hash = await bcrypt.hash(`LeadTeam${i}@2026`, 12);
    await prisma.user.upsert({
      where: { username: `lead_team_${i}` },
      update: {},
      create: {
        username: `lead_team_${i}`,
        password: hash,
        displayName: `Lead TEAM_${i}`,
        team: `TEAM_${i}`,
        role: "leader",
      },
    });
  }

  // Sample employee
  await prisma.user.upsert({
    where: { username: "emp_thang" },
    update: {},
    create: {
      username: "emp_thang",
      password: empPassword,
      displayName: "Thắng",
      team: "TEAM_3",
      role: "viewer",
    },
  });

  console.log("✅ Seed complete");
}

main()
  .catch((e) => { console.error(e); process.exit(1); })
  .finally(() => prisma.$disconnect());
