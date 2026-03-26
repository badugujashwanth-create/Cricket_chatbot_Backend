const fs = require('fs');
const path = require('path');

let loaded = false;

function stripQuotes(value = '') {
  const text = String(value || '').trim();
  if (
    (text.startsWith('"') && text.endsWith('"')) ||
    (text.startsWith("'") && text.endsWith("'"))
  ) {
    return text.slice(1, -1);
  }
  return text;
}

function loadEnv(filename = '.env') {
  if (loaded) return;
  loaded = true;

  const envPath = path.join(__dirname, filename);
  if (!fs.existsSync(envPath)) return;

  const content = fs.readFileSync(envPath, 'utf8');
  const lines = content.split(/\r?\n/);

  for (const line of lines) {
    const trimmed = String(line || '').trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex <= 0) continue;

    const key = trimmed.slice(0, separatorIndex).trim();
    if (!key || Object.prototype.hasOwnProperty.call(process.env, key)) continue;

    const rawValue = trimmed.slice(separatorIndex + 1);
    process.env[key] = stripQuotes(rawValue);
  }
}

loadEnv();

module.exports = {
  loadEnv
};
