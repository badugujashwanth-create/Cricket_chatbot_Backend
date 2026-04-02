const fs = require('fs');
const path = require('path');

const { normalizeText, tokenize } = require('./textUtils');

const PLAYER_MASTER_PATH = path.join(__dirname, 'player_master.json');

let aliasMap = null;
let canonicalAliasMap = null;

function loadAliasMap() {
  if (aliasMap) return aliasMap;

  const map = new Map();
  const reverseMap = new Map();
  if (fs.existsSync(PLAYER_MASTER_PATH)) {
    try {
      const payload = JSON.parse(fs.readFileSync(PLAYER_MASTER_PATH, 'utf8'));
      for (const [alias, canonical] of Object.entries(payload || {})) {
        const cleanAlias = normalizeText(alias);
        const cleanCanonical = String(canonical || '').trim();
        if (!cleanAlias || !cleanCanonical) continue;
        map.set(cleanAlias, cleanCanonical);
        map.set(normalizeText(cleanCanonical), cleanCanonical);
        const reverseKey = normalizeText(cleanCanonical);
        const aliasSet = reverseMap.get(reverseKey) || new Set();
        aliasSet.add(String(alias || '').trim());
        aliasSet.add(cleanCanonical);
        reverseMap.set(reverseKey, aliasSet);
      }
    } catch (_) {
      // Fall back to generated aliases only when the manual map is unreadable.
    }
  }

  aliasMap = map;
  canonicalAliasMap = reverseMap;
  return aliasMap;
}

function getCanonicalPlayerName(value = '') {
  return loadAliasMap().get(normalizeText(value || '')) || '';
}

function getManualAliasesForCanonical(value = '') {
  loadAliasMap();
  const canonical = getCanonicalPlayerName(value) || String(value || '').trim();
  return [...(canonicalAliasMap?.get(normalizeText(canonical)) || new Set())];
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
  getManualAliasesForCanonical(name).forEach((alias) => addNameVariants(aliases, alias));
  return [...aliases];
}

module.exports = {
  buildPlayerAliases,
  getCanonicalPlayerName
};
