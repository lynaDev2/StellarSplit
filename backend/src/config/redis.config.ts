import { ConfigService } from "@nestjs/config";

type RedisConnectionOptions = {
  host: string;
  port: number;
  username?: string;
  password?: string;
};

function getString(
  configService: Pick<ConfigService, "get">,
  ...keys: string[]
): string | undefined {
  for (const key of keys) {
    const value = configService.get<string>(key);
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }

  return undefined;
}

export function getRedisConnectionOptions(
  configService: Pick<ConfigService, "get">,
): RedisConnectionOptions {
  const redisUrl = getString(configService, "REDIS_URL");

  if (redisUrl) {
    const parsed = new URL(redisUrl);
    return {
      host: parsed.hostname,
      port: parsed.port ? parseInt(parsed.port, 10) : 6379,
      ...(parsed.username
        ? { username: decodeURIComponent(parsed.username) }
        : {}),
      ...(parsed.password
        ? { password: decodeURIComponent(parsed.password) }
        : {}),
    };
  }

  const host =
    getString(configService, "REDIS_HOST", "REDISHOST") ?? "localhost";
  const rawPort =
    getString(configService, "REDIS_PORT", "REDISPORT") ?? "6379";
  const username = getString(configService, "REDIS_USERNAME", "REDISUSER");
  const password = getString(
    configService,
    "REDIS_PASSWORD",
    "REDISPASSWORD",
  );

  return {
    host,
    port: parseInt(rawPort, 10),
    ...(username ? { username } : {}),
    ...(password ? { password } : {}),
  };
}
