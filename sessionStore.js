const crypto = require('crypto');

const TTL_MS = 60 * 60 * 1000;
const sessions = new Map();

function pruneSessions() {
  const now = Date.now();
  for (const [id, session] of sessions.entries()) {
    if (!session || now - session.updatedAt > TTL_MS) {
      sessions.delete(id);
    }
  }
}

function createSession(id) {
  return {
    id: id || crypto.randomUUID(),
    updatedAt: Date.now(),
    context: {
      player_id: '',
      player_name: '',
      team_id: '',
      team_name: '',
      venue: '',
      season: '',
      format: '',
      action: ''
    },
    pendingClarification: null
  };
}

function getSession(id) {
  pruneSessions();
  if (id && sessions.has(id)) {
    const session = sessions.get(id);
    session.updatedAt = Date.now();
    return session;
  }
  const created = createSession(id);
  sessions.set(created.id, created);
  return created;
}

function setPendingClarification(session, pending) {
  if (!session) return;
  session.pendingClarification = pending || null;
  session.updatedAt = Date.now();
}

function clearPendingClarification(session) {
  if (!session) return;
  session.pendingClarification = null;
  session.updatedAt = Date.now();
}

function updateContext(session, patch = {}) {
  if (!session) return;
  session.context = {
    ...session.context,
    ...patch
  };
  session.updatedAt = Date.now();
}

module.exports = {
  getSession,
  setPendingClarification,
  clearPendingClarification,
  updateContext
};
