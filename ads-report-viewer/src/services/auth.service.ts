import { prisma } from "@/lib/prisma";
import { signToken } from "@/lib/jwt";
import bcrypt from "bcryptjs";
import type { Role } from "@/types";

export interface RegisterInput {
  username: string;
  password: string;
  confirmPassword: string;
  team?: string;
  displayName?: string;
  // telegramId is NOT collected during registration — use Connect Telegram flow
}

export interface LoginInput {
  username: string;
  password: string;
}

export async function registerUser(input: RegisterInput) {
  const { username, password, confirmPassword, team = "", displayName = "" } = input;

  if (password !== confirmPassword) {
    throw new Error("Passwords do not match");
  }

  if (password.length < 6) {
    throw new Error("Password must be at least 6 characters");
  }

  const existing = await prisma.user.findUnique({ where: { username } });
  if (existing) {
    throw new Error("Username already taken");
  }

  const hashed = await bcrypt.hash(password, 12);

  const user = await prisma.user.create({
    data: {
      username,
      password: hashed,
      displayName: displayName || username,
      team,
      role: "viewer",
    },
  });

  return { id: user.id, username: user.username, role: user.role };
}

export async function loginUser(input: LoginInput): Promise<string> {
  const { username, password } = input;

  const user = await prisma.user.findUnique({ where: { username } });
  if (!user) {
    throw new Error("Invalid credentials");
  }

  const valid = await bcrypt.compare(password, user.password);
  if (!valid) {
    throw new Error("Invalid credentials");
  }

  const token = await signToken({
    userId: user.id,
    username: user.username,
    role: user.role as Role,
    team: user.team,
  });

  return token;
}

export async function getUserById(id: string) {
  return prisma.user.findUnique({
    where: { id },
    select: {
      id: true,
      username: true,
      displayName: true,
      team: true,
      role: true,
      telegramId: true,
      telegramUsername: true,
      createdAt: true,
    },
  });
}
