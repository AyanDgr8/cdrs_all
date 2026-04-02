// apiDataFetcher.js
// Fetches CDR data from multiple endpoints and populates database

import dotenv from 'dotenv';
dotenv.config();

import dbService from './dbService.js';
import { getPortalToken, getTenantConfig } from './tokenService.js';

const MAX_PAGES = 25000;

const CDR_ENDPOINT = '/api/v2/reports/cdrs/all';

// Removed classification - all CDRs go to raw_cdrs_all table

/**
 * Main function: fetch CDRs from cdrs/all endpoint and classify them
 * @param {string} tenant - Tenant key (e.g. 'meydan')
 * @param {Object} params - { startDate, endDate } (Unix timestamps in seconds)
 * @returns {Promise<Object>} - Summary with totalRecords, totalPages, totalTime
 */
export async function fetchAllAPIsAndPopulateDB(tenant, params) {
  console.log('🚀 Starting CDR fetch for tenant:', tenant);
  const startTime = Date.now();

  try {
    const result = await fetchCdrsWithPagination(tenant, params);
    const summary = {
      totalTime: Date.now() - startTime,
      results: { cdrs_all: result }
    };
    console.log(`🎉 CDR fetch completed in ${summary.totalTime}ms — ${result.totalRecords} records`);
    return summary;
  } catch (error) {
    console.error('❌ Error fetching CDRs:', error);
    throw error;
  }
}

/**
 * Fetch CDRs with full pagination and classify/store each page in DB immediately
 */
async function fetchCdrsWithPagination(tenant, params) {
  let totalRecords = 0;
  let nextStartKey = null;
  let pageCount = 0;
  
  // All records go to raw_cdrs_all table

  while (pageCount < MAX_PAGES) {
    const requestParams = { ...params };
    if (nextStartKey) {
      requestParams.startKey = nextStartKey;
    }

    try {
      const response = await fetchPage(tenant, requestParams);
      const data = response.cdrs || response.data || [];
      const newStartKey = response.next_start_key;

      console.log(`📝 Page ${pageCount + 1}: ${data.length} records, next_start_key: "${newStartKey || ''}"`);

      if (data.length === 0) {
        console.log(`✅ No more data — ${totalRecords} total records in ${pageCount + 1} pages`);
        break;
      }

      // Store all CDRs in raw_cdrs_all table
      try {
        await dbService.batchInsertRawCdrsAll(data, tenant);
        console.log(`💾 Page ${pageCount + 1} inserted: ${data.length} records into raw_cdrs_all_${tenant}`);
      } catch (dbError) {
        console.error(`❌ DB error on page ${pageCount + 1}:`, dbError.message);
      }

      totalRecords += data.length;
      pageCount++;

      // Stop if pagination is complete
      if (!newStartKey || newStartKey === '' || newStartKey === null) {
        console.log(`✅ Pagination complete — ${totalRecords} records in ${pageCount} pages`);
        break;
      }

      nextStartKey = newStartKey;
    } catch (error) {
      console.error(`❌ Error on page ${pageCount + 1}:`, error.message);

      // Retry on transient / auth errors
      if (error.message.includes('timeout') || error.message.includes('ECONNRESET') ||
          error.message.includes('502') || error.message.includes('503') ||
          error.message.includes('RETRY_AUTH')) {
        console.log(`🔄 Retrying page ${pageCount + 1}...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
        continue;
      }
      break;
    }
  }

  if (pageCount >= MAX_PAGES) {
    console.log(`⚠️ Reached max page limit (${MAX_PAGES}). Total: ${totalRecords} records`);
  }

  console.log(`📊 Total records inserted into raw_cdrs_all_${tenant}: ${totalRecords}`);

  return { endpoint: CDR_ENDPOINT, totalRecords, totalPages: pageCount };
}

/**
 * Format date value to Unix timestamp in seconds for the API
 */
function formatDateForApi(dateString) {
  if (!dateString) return null;
  if (typeof dateString === 'number') return dateString;
  try {
    const date = dateString instanceof Date ? dateString : new Date(dateString);
    return Math.floor(date.getTime() / 1000);
  } catch (e) {
    console.error('Error formatting date:', e);
    return dateString;
  }
}

/**
 * Fetch a single page from the CDR API
 */
async function fetchPage(tenant, params) {
  const token = await getPortalToken(tenant);
  if (!token) {
    throw new Error('Failed to obtain authentication token');
  }

  const formattedParams = { ...params };

  // Normalise date param names
  if (formattedParams.start_date) {
    formattedParams.startDate = formatDateForApi(formattedParams.start_date);
    delete formattedParams.start_date;
  }
  if (formattedParams.end_date) {
    formattedParams.endDate = formatDateForApi(formattedParams.end_date);
    delete formattedParams.end_date;
  }

  formattedParams.pageSize = 2000;

  const { base_url, account_id } = getTenantConfig(tenant);

  const queryParams = new URLSearchParams({
    account: account_id,
    ...formattedParams
  });

  const fullUrl = `${base_url}${CDR_ENDPOINT}?${queryParams}`;
  console.log(`🔍 API Request: ${fullUrl}`);

  const response = await fetch(fullUrl, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      'X-Account-ID': account_id
    },
    signal: AbortSignal.timeout(30000)
  });

  console.log(`📡 Response: ${response.status} ${response.statusText}`);

  if (!response.ok) {
    const errorText = await response.text();
    if (response.status === 401) {
      throw new Error(`RETRY_AUTH:API request failed: ${response.status} - ${errorText}`);
    }
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const responseData = await response.json();
  if (!responseData) {
    throw new Error('API returned null/undefined response');
  }

  return responseData;
}

export default {
  fetchAllAPIsAndPopulateDB
};
