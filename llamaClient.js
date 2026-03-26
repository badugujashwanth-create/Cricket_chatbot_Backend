require('./loadEnv');

const LOCAL_ENDPOINT =
  process.env.LLM_ENDPOINT ||
  (process.env.LLM_BASE_URL
    ? `${String(process.env.LLM_BASE_URL).replace(/\/+$/, '')}/chat/completions`
    : 'http://localhost:8080/v1/chat/completions');
const LOCAL_MODEL = process.env.LLM_MODEL || 'local';
const DEFAULT_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 30000);

const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-4o').trim();
const OPENAI_ENDPOINT = String(
  process.env.OPENAI_ENDPOINT || 'https://api.openai.com/v1/chat/completions'
).trim();
const providerHealth = new Map();

function normalizeChatCompletionsUrl(value = '') {
  const raw = String(value || '').trim().replace(/\/+$/, '');
  if (!raw) return '';
  if (raw.endsWith('/chat/completions')) return raw;
  return `${raw}/chat/completions`;
}

function localEndpointUrl() {
  return normalizeChatCompletionsUrl(LOCAL_ENDPOINT) || 'http://localhost:8080/v1/chat/completions';
}

function openAiEndpointUrl() {
  return normalizeChatCompletionsUrl(OPENAI_ENDPOINT) || 'https://api.openai.com/v1/chat/completions';
}

function hasLocalLlm() {
  return Boolean(String(LOCAL_ENDPOINT || '').trim());
}

function hasOpenAiLlm() {
  return Boolean(OPENAI_API_KEY);
}

function providerCooldownMs(error) {
  const message = String(error?.message || error || '').toLowerCase();
  if (message.includes('insufficient_quota') || message.includes('429')) {
    return 30 * 60 * 1000;
  }
  if (message.includes('unable to connect') || message.includes('econnrefused')) {
    return 2 * 60 * 1000;
  }
  if (message.includes('aborted') || message.includes('timeout')) {
    return 60 * 1000;
  }
  return 5 * 60 * 1000;
}

function providerReady(provider) {
  const state = providerHealth.get(provider);
  if (!state) return true;
  if (state.retryAt <= Date.now()) {
    providerHealth.delete(provider);
    return true;
  }
  return false;
}

function markProviderFailure(provider, error) {
  providerHealth.set(provider, {
    retryAt: Date.now() + providerCooldownMs(error),
    message: String(error?.message || error || '')
  });
}

function markProviderSuccess(provider) {
  providerHealth.delete(provider);
}

function providerOrder({ provider = 'auto', purpose = 'general' } = {}) {
  if (provider === 'local') return hasLocalLlm() && providerReady('local') ? ['local'] : [];
  if (provider === 'openai') return hasOpenAiLlm() && providerReady('openai') ? ['openai'] : [];

  if (purpose === 'router') {
    return [
      hasLocalLlm() && providerReady('local') ? 'local' : '',
      hasOpenAiLlm() && providerReady('openai') ? 'openai' : ''
    ].filter(Boolean);
  }

  return [
    hasOpenAiLlm() && providerReady('openai') ? 'openai' : '',
    hasLocalLlm() && providerReady('local') ? 'local' : ''
  ].filter(Boolean);
}

async function callChatCompletions({
  endpoint,
  model,
  apiKey = '',
  messages = [],
  timeoutMs = DEFAULT_TIMEOUT_MS,
  temperature = 0
}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(timeoutMs || DEFAULT_TIMEOUT_MS));

  try {
    const headers = {
      'Content-Type': 'application/json'
    };
    if (apiKey) {
      headers.Authorization = `Bearer ${apiKey}`;
    }

    const response = await fetch(endpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model,
        temperature: Number.isFinite(Number(temperature)) ? Number(temperature) : 0,
        messages
      }),
      signal: controller.signal
    });

    if (!response.ok) {
      const payload = await response.text().catch(() => '');
      throw new Error(`LLM request failed (${response.status})${payload ? `: ${payload}` : ''}`);
    }

    const payload = await response.json();
    return payload?.choices?.[0]?.message?.content || '';
  } finally {
    clearTimeout(timeout);
  }
}

async function callProvider(provider, messages = [], options = {}) {
  const temperature = options.temperature;
  const timeoutMs = Number(options.timeoutMs || DEFAULT_TIMEOUT_MS);

  if (provider === 'openai') {
    if (!hasOpenAiLlm()) {
      throw new Error('OpenAI API key is not configured.');
    }
    return callChatCompletions({
      endpoint: openAiEndpointUrl(),
      model: String(options.model || OPENAI_MODEL || 'gpt-4o').trim(),
      apiKey: OPENAI_API_KEY,
      messages,
      temperature,
      timeoutMs
    });
  }

  if (provider === 'local') {
    return callChatCompletions({
      endpoint: localEndpointUrl(),
      model: String(options.model || LOCAL_MODEL || 'local').trim(),
      messages,
      temperature,
      timeoutMs
    });
  }

  throw new Error(`Unsupported LLM provider: ${provider}`);
}

async function callLlama(messages = [], options = {}) {
  const order = providerOrder(options);
  if (!order.length) {
    throw new Error('No LLM provider is configured.');
  }

  let lastError = null;
  for (const provider of order) {
    try {
      const content = await callProvider(provider, messages, options);
      markProviderSuccess(provider);
      return content;
    } catch (error) {
      lastError = error;
      markProviderFailure(provider, error);
    }
  }

  throw lastError || new Error('Unable to reach any configured LLM provider.');
}

function getLlmConfigSummary() {
  return {
    local_endpoint: localEndpointUrl(),
    local_model: LOCAL_MODEL,
    openai_enabled: hasOpenAiLlm(),
    openai_model: OPENAI_MODEL,
    provider_order: {
      router: providerOrder({ purpose: 'router' }),
      reasoning: providerOrder({ purpose: 'reasoning' })
    },
    health: Object.fromEntries(
      [...providerHealth.entries()].map(([provider, state]) => [
        provider,
        {
          retry_at: new Date(state.retryAt).toISOString(),
          message: state.message
        }
      ])
    )
  };
}

module.exports = {
  callLlama,
  localEndpointUrl,
  openAiEndpointUrl,
  getLlmConfigSummary
};
