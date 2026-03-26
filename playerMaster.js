const fs = require('fs');
const path = require('path');

const { normalizeText, tokenize } = require('./textUtils');

const PLAYER_MASTER_PATH = path.join(__dirname, 'player_master.json');

let aliasMap = null;

function loadAliasMap() {
  if (aliasMap) return aliasMap;

  const map = new Map();
  if (fs.existsSync(PLAYER_MASTER_PATH)) {
    try {
      const payload = JSON.parse(fs.readFileSync(PLAYER_MASTER_PATH, 'utf8'));
      for (const [alias, canonical] of Object.entries(payload || {})) {
        const cleanAlias = normalizeText(alias);
        const cleanCanonical = String(canonical || '').trim();
        if (!cleanAlias || !cleanCanonical) continue;
        map.set(cleanAlias, cleanCanonical);
        map.set(normalizeText(cleanCanonical), cleanCanonical);
      }
    } catch (_) {
      // Fall back to generated aliases only when the manual map is unreadable.
    }
  }

  aliasMap = map;
  return aliasMap;
}

function getCanonicalPlayerName(value = '') {
  return loadAliasMap().get(normalizeText(value || '')) || '';
}

function addNameVariants(target, value = '') {
  const cleanValue = String(value || '').trim();
  if (!cleanValue) return;
  target.add(cleanValue);

  const parts = tokenize(cleanValue);
  if (parts.length >= 2) {
    const first = parts[0];
    const last = parts[parts.length - 1];
    const firstInitial = (first[0] || '').toUpperCase();
    if (firstInitial && last) {
      target.add(`${firstInitial} ${last}`);
      target.add(`${firstInitial}. ${last}`);
      target.add(`${last} ${firstInitial}`);
      target.add(`${last}, ${firstInitial}`);
    }
    target.add(last);
  }
}

function buildPlayerAliases(name = '') {
  const aliases = new Set();
  addNameVariants(aliases, name);
  const canonical = getCanonicalPlayerName(name);
  if (canonical) {
    addNameVariants(aliases, canonical);
  }
  return [...aliases];
}

module.exports = {
  buildPlayerAliases,
  getCanonicalPlayerName
};
