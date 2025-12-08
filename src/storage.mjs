// Ensure kv table exists with NOT NULL updated_at
async function ensureKv(env) {
  await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT NOT NULL
    );
  `).run();
}

export async function setJson(env, key, value) {
  await ensureKv(env);
  const text = JSON.stringify(value ?? null);
  const now = new Date().toISOString(); // fits TEXT NOT NULL

  await env.D1_DB.prepare(
    `
    INSERT INTO kv (key, value, updated_at)
    VALUES (?1, ?2, ?3)
    ON CONFLICT(key) DO UPDATE SET
      value = excluded.value,
      updated_at = excluded.updated_at
    `
  )
    .bind(key, text, now)
    .run();
}

export async function getJson(env, key, defaultValue) {
  await ensureKv(env);
  const row = await env.D1_DB.prepare(
    "SELECT value FROM kv WHERE key = ?1"
  )
    .bind(key)
    .first();

  if (!row) return defaultValue;
  try {
    return JSON.parse(row.value);
  } catch {
    return defaultValue;
  }
}
