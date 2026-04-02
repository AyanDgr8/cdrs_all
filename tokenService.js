// tokenService.js
import dotenv from 'dotenv';
import axios from 'axios';
import ms from 'ms';
import https from 'https';

// Load environment variables from .env file
dotenv.config();

const cache = new Map();        // In prod use Redis
const MAX_RETRIES = 3;

// Optional: accept self-signed certificates in dev; set NODE_TLS_REJECT_UNAUTHORIZED=1 in prod
const httpsAgent = new https.Agent({ rejectUnauthorized: false });

// Cache structure: Map<tenant, {access, refresh, expiresAt}>

// ─── Multi-Tenant Configuration ───
// Add new tenants here. The key (lowercase) is used as the table suffix in the DB.
export const TENANT_BASE_HEADER = {
  // dsouth: {
  //   name: 'Dsouth',
  //   base_url: 'https://ira-ds-du.ucprem.voicemeetme.com:9443',
  //   account_id: 'e16737db698d2ff469956b77a17a7c62',
  //   domain: 'dsouth'
  // },
  meydan: {
    name: 'Meydan',
    base_url: 'https://ira-meydan-du.ucprem.voicemeetme.com:9443',
    account_id: '544370e9c33f4d679f0f1d37778d3c7c',
    domain: 'meydan'
  },
  shams: {
    name: 'Shams',
    base_url: 'https://uc.ira-shams-sj.ucprem.voicemeetme.com:9443',
    account_id: '08298de66d77d57def3fe8c5fd90db0f',
    domain: 'shams'
  },
  // cdr: {
  //   name: 'CDR',
  //   base_url: 'https://ucdemo.voicemeetme.com:9443',
  //   account_id: '49ae0694dc527ac29e5ce1c2b1dbd3f5',
  //   domain: 'cdr'
  // }
};

// Common credentials shared across all tenants (loaded from .env)
const API_USERNAME = process.env.API_USERNAME || 'ayan@multycomm.com';
const API_PASSWORD = process.env.API_PASSWORD || 'WELcome@123';

/**
 * Get configuration for a given tenant key.
 * @param {string} tenant - tenant key, e.g. 'dsouth' or 'meydan'
 * @returns {{ name, base_url, account_id, domain }}
 */
export function getTenantConfig(tenant) {
  const key = (tenant || '').toLowerCase();
  const config = TENANT_BASE_HEADER[key];
  if (!config) {
    throw new Error(`Unknown tenant "${tenant}". Please select a valid tenant from the dropdown.`);
  }
  return config;
}

/**
 * Fetch an access token using the legacy call center login endpoint.
 * Falls back to the cached token until two minutes before expiry.
 *
 * @param {string} tenant - tenant / domain, e.g. `mc_int`.
 * @returns {Promise<string>} access token (JWT)
 */
export async function getToken(tenant) {
  const now = Date.now();
  const cached = cache.get(tenant);
  if (cached && now < cached.expiresAt - ms('2m')) return cached.access;

  const { base_url, domain } = getTenantConfig(tenant);

  for (let i = 0, delay = 1000; i < MAX_RETRIES; i++, delay *= 2) {
    try {
      const {data} = await axios.post(
        `${base_url}/portal/callcenter/reports/agents-status-activity`,
        { username: API_USERNAME, password: API_PASSWORD, domain },
        { timeout: 5000, httpsAgent, headers: { Accept: 'application/json' } }
      );
      cache.set(tenant, {
        access: data.access_token,
        refresh: data.refresh_token,
        expiresAt: now + ms('1h')
      });
      return data.access_token;
    } catch (err) {
      if (i === MAX_RETRIES - 1) throw err;
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

/**
 * Fetch an access token using the modern portal login endpoint that the web
 * UI employs. This token is accepted by the new `/api/v2/reports/...` routes.
 * Falls back to the cached token until two minutes before expiry.
 *
 * @param {string} tenant - tenant / domain, e.g. `mc_int`.
 * @returns {Promise<string>} access token (JWT)
 */
export async function getPortalToken(tenant) {
  const now = Date.now();
  const cached = cache.get(`portal:${tenant}`);
  if (cached && now < cached.expiresAt - ms('2m')) {
    console.log(`⚡ Token cache HIT for tenant: ${tenant}`);
    return cached.access;
  }
  console.log(`🔄 Token cache MISS for tenant: ${tenant}, fetching new token...`);

  // Back-off loop across candidate endpoints / payloads
  const { base_url, domain } = getTenantConfig(tenant);
  const candidates = [
    { url: `${base_url}/api/v2/config/login/oauth`, body: { domain, username: API_USERNAME, password: API_PASSWORD } },
    { url: `${base_url}/api/v2/login`, body: { domain, username: API_USERNAME, password: API_PASSWORD } },
    { url: `${base_url}/api/login`, body: { domain, username: API_USERNAME, password: API_PASSWORD } },
  ];

  for (const { url, body } of candidates) {
    for (let attempt = 0, delay = 1000; attempt < MAX_RETRIES; attempt++, delay *= 2) {
      try {
        const { data } = await axios.post(url, body, { 
          timeout: 5000,
          httpsAgent,
          headers: { Accept: 'application/json' }
        });

        const access = data.accessToken || data.access_token;
        if (!access) throw new Error('No access token in response');

        const refresh = data.refreshToken || data.refresh_token;
        const expiresAt = data.expiresIn ? Date.now() + data.expiresIn * 1000 : Date.now() + ms('1h');

        cache.set(`portal:${tenant}`, { access, refresh, expiresAt });
        console.log(`✅ Portal login succeeded at ${url}`);
        return access;
      } catch (err) {
        if (attempt === MAX_RETRIES - 1) {
          // try next candidate endpoint
          if (process.env.DEBUG) {
            console.warn(`Login failed at ${url}: ${err.response?.status || err.message}`);
          }
        } else {
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
  }
  throw new Error('All portal login attempts failed – check credentials/endpoints');
}

export { httpsAgent };
