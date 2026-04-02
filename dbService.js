// dbService.js
// Comprehensive database service for SPC CDR reporting system

import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

// Create a connection pool

const pool = mysql.createPool({
  host:  process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'Ayan@1012',
  database: process.env.DB_NAME || 'CDR_ALL_db',
  port: 3306,
  waitForConnections: true,
  connectionLimit: 50,  // Increased for better multi-tab performance
  queueLimit: 25,       // Added queue limit to prevent overwhelming the server
  multipleStatements: true,
  connectTimeout: 60000,  // 60 seconds connection timeout
  acquireTimeout: 60000,  // 60 seconds acquire timeout
  timeout: 180000,       // 180 seconds query timeout
  enableKeepAlive: true, // Enable connection keep-alive
  keepAliveInitialDelay: 10000 // Keep-alive ping every 10 seconds
});


// ─── Multi-Tenant Helpers ───

/**
 * Sanitize a tenant key to a safe table suffix (lowercase alphanumeric + underscore).
 * @param {string} tenant
 * @returns {string} e.g. 'dsouth', 'meydan', 'cogent'
 */
export function getTenantSuffix(tenant) {
  if (!tenant || tenant === 'null' || tenant === 'undefined') {
    throw new Error('Tenant parameter is required. Please select a tenant from the dropdown.');
  }
  return String(tenant).toLowerCase().replace(/[^a-z0-9_]/g, '');
}

/**
 * Return all tenant-specific table names for the given tenant key.
 * @param {string} tenant - e.g. 'dsouth' or 'meydan'
 * @returns {{ rawCdrsAll, rawInbound, rawOutbound, rawCampaign, finalReport }}
 */
export function getTenantTables(tenant) {
  const suffix = getTenantSuffix(tenant);
  return {
    rawCdrsAll:  `raw_cdrs_all_${suffix}`,
    rawInbound:  `raw_inbound_${suffix}`,
    rawOutbound: `raw_outbound_${suffix}`,
    rawCampaign: `raw_campaign_${suffix}`,
    finalReport: `final_report_${suffix}`
  };
}

/**
 * Normalize a phone number for consistent comparison
 * @param {string} phoneNumber - Phone number to normalize
 * @returns {string} - Normalized phone number (digits only, last 10 digits)
 */
function normalizePhoneNumber(phoneNumber) {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return '';
  }
  
  // Remove all non-digit characters
  let normalized = phoneNumber.replace(/\D/g, '');
  
  // Remove leading zeros
  normalized = normalized.replace(/^0+/, '');
  
  // Handle extensions by taking last 10 digits if longer than 10
  if (normalized.length > 10) {
    normalized = normalized.slice(-10);
  }
  
  return normalized;
}

// Utility function to convert timestamps to Unix seconds for database queries
function convertTimestamp(timestamp) {
  if (timestamp === null || timestamp === undefined) {
    return null;
  }

  // If it's already a Date object
  if (timestamp instanceof Date) {
    return Math.floor(timestamp.getTime() / 1000); // Return Unix seconds
  }

  // If it's a number (seconds or milliseconds)
  if (typeof timestamp === 'number') {
    // If it's already in seconds, return as-is; if milliseconds, convert to seconds
    return timestamp < 10000000000 ? timestamp : Math.floor(timestamp / 1000);
  }

  // If it's a string, try to parse it
  if (typeof timestamp === 'string') {
    const parsed = Date.parse(timestamp);
    if (!isNaN(parsed)) {
      return Math.floor(parsed / 1000); // Return Unix seconds
    }
  }

  return null; // Return null for invalid timestamps
}

/**
 * Helper function to normalize date parameters to support both naming conventions
 * @param {Object} filters - Filter object containing date parameters
 * @returns {Object} - Object with normalized startDate and endDate properties
 */
function normalizeDateParams(filters = {}) {
  return {
    startDate: filters.start_date || filters.startDate || null,
    endDate: filters.end_date || filters.endDate || null
  };
}

// Extract raw timestamp value for BIGINT columns
function extractRawTimestamp(timestamp) {
  if (!timestamp) return null;
  
  let result = null;
  const originalValue = timestamp;
  
  if (typeof timestamp === 'number') {
    // For BIGINT columns, we want the raw Unix timestamp in seconds
    result = timestamp > 1e10 ? Math.floor(timestamp / 1000) : timestamp;
  } else if (typeof timestamp === 'string') {
    // Try to convert string to timestamp
    const date = new Date(timestamp);
    result = isNaN(date.getTime()) ? null : Math.floor(date.getTime() / 1000);
  }
  
  // // Add debug logging for timestamp conversion
  // console.log(`Timestamp conversion: ${typeof originalValue} ${originalValue} -> ${result}`);
  
  return result;
}

/**
 * Execute a query with parameters
 * @param {string} sql - SQL query
 * @param {Array} params - Query parameters
 * @returns {Promise<Array>} - Query results
 */
async function query(sql, params) {
  const startTime = Date.now();
  let connection;
  let retryCount = 0;
  const maxRetries = 3;
  const initialBackoff = 100; // Start with 100ms backoff
  
  while (retryCount <= maxRetries) {
    try {
      // Get a connection from the pool with timeout protection
      const connectionPromise = pool.getConnection();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Connection acquisition timeout')), 10000); // 10 second timeout
      });
      
      try {
        connection = await Promise.race([connectionPromise, timeoutPromise]);
      } catch (connError) {
        if (connError.message === 'Connection acquisition timeout') {
          console.warn(`⚠️ Connection acquisition timeout, retry ${retryCount + 1}/${maxRetries}`);
          retryCount++;
          if (retryCount <= maxRetries) {
            await new Promise(resolve => setTimeout(resolve, initialBackoff * Math.pow(2, retryCount)));
            continue;
          }
        }
        throw connError;
      }
      
      // Execute the query with timeout protection
      const [rows] = await connection.execute(sql, params);
      
      const duration = Date.now() - startTime;
      if (duration > 1000) { // Only log slow queries (>1s)
        console.log(`✅ Query executed in ${duration}ms, returned ${rows?.length || 0} rows`);
      }
      
      return rows;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Check if error is retryable
      const isRetryable = 
        error.code === 'ETIMEDOUT' || 
        error.code === 'ECONNRESET' || 
        error.code === 'ER_LOCK_WAIT_TIMEOUT' ||
        error.message.includes('too many connections');
      
      if (isRetryable && retryCount < maxRetries) {
        retryCount++;
        const backoffTime = initialBackoff * Math.pow(2, retryCount);
        console.warn(`⚠️ Retryable database error (${error.code}), attempt ${retryCount}/${maxRetries} after ${backoffTime}ms delay`);
        await new Promise(resolve => setTimeout(resolve, backoffTime));
        continue;
      }
      
      // Categorize and log the error
      if (error.code === 'ETIMEDOUT' || error.code === 'ECONNRESET') {
        console.error(`❌ Database connection error (${error.code}) after ${duration}ms:`, error.message);
      } else if (error.code === 'ER_LOCK_WAIT_TIMEOUT') {
        console.error(`❌ Database lock timeout after ${duration}ms:`, error.message);
      } else {
        console.error(`❌ Database query error after ${duration}ms:`, error.message);
      }
      
      // Enhance the error with more context
      error.queryDuration = duration;
      error.sql = sql.substring(0, 200) + (sql.length > 200 ? '...' : '');
      throw error;
    } finally {
      // Always release the connection back to the pool
      if (connection) {
        connection.release();
      }
    }
  }
}

/**
 * Execute optimized bulk inserts using VALUES clause for maximum performance
 * @param {string} sql - Base SQL query (INSERT INTO table (columns))
 * @param {Array<Array>} batchParams - Array of parameter arrays for each record
 * @param {number} chunkSize - Number of records per bulk insert (default: 1000)
 * @returns {Promise<Object>} - Result object
 */
async function batchInsert(sql, batchParams, chunkSize = 1000) {
  if (!batchParams || batchParams.length === 0) {
    return { affectedRows: 0 };
  }
  
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    
    let totalAffected = 0;
    
    // Process in chunks for optimal performance
    for (let i = 0; i < batchParams.length; i += chunkSize) {
      const chunk = batchParams.slice(i, i + chunkSize);
      
      // Build bulk VALUES clause
      const placeholders = chunk.map(() => `(${chunk[0].map(() => '?').join(', ')})`).join(', ');
      const bulkSql = `${sql} VALUES ${placeholders}`;
      
      // Flatten parameters for bulk insert
      const flatParams = chunk.flat();
      
      const [result] = await connection.execute(bulkSql, flatParams);
      totalAffected += result.affectedRows;
    }
    
    await connection.commit();
    return { affectedRows: totalAffected };
  } catch (error) {
    await connection.rollback();
    console.error('Bulk insert error:', error);
    throw error;
  } finally {
    connection.release();
  }
}

/**
 * Begin a transaction
 * @returns {Promise<mysql.Connection>} - Connection with active transaction
 */
export async function beginTransaction() {
  const connection = await pool.getConnection();
  await connection.beginTransaction();
  return connection;
}

/**
 * Commit a transaction
 * @param {mysql.Connection} connection - Connection with active transaction
 */
export async function commitTransaction(connection) {
  try {
    await connection.commit();
  } finally {
    connection.release();
  }
}

/**
 * Rollback a transaction
 * @param {mysql.Connection} connection - Connection with active transaction
 */
export async function rollbackTransaction(connection) {
  try {
    await connection.rollback();
  } finally {
    connection.release();
  }
}


/**
 * Batch insert raw CDR data into raw_cdrs_all table
 * @param {Array<Object>} cdrsData - Array of raw CDR data objects from /api/v2/reports/cdrs/all
 * @param {string} tenant - Tenant key
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawCdrsAll(cdrsData, tenant) {
  if (!cdrsData || cdrsData.length === 0) {
    return { affectedRows: 0 };
  }

  const tableName = getTenantTables(tenant).rawCdrsAll;
  
  const batchParams = cdrsData.map(cdr => {
    const callId = cdr.call_id || cdr.callid || null;
    const timestamp = extractRawTimestamp(cdr.timestamp || cdr.channel_created_time) || null;
    const rawData = JSON.stringify(cdr);
    
    return [callId, timestamp, rawData];
  });
  
  const baseSql = `INSERT IGNORE INTO ${tableName} (
    call_id, timestamp, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} CDR records into ${tableName}`);
    return result;
  } catch (error) {
    console.error('Error batch inserting raw CDR data:', error);
    throw error;
  }
}

/**
 * Batch insert queue inbound CDR data into raw_inbound table
 * @param {Array<Object>} cdrsData - Array of raw CDR data objects from queue inbound endpoint
 * @param {string} tenant - Tenant key
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawInbound(cdrsData, tenant) {
  if (!cdrsData || cdrsData.length === 0) {
    return { affectedRows: 0 };
  }

  const tableName = getTenantTables(tenant).rawInbound;
  
  const batchParams = cdrsData.map(cdr => {
    const callId = cdr.callid || cdr.a_leg || cdr.call_id || null;
    const timestamp = extractRawTimestamp(cdr.timestamp || cdr.called_time) || null;
    const rawData = JSON.stringify(cdr);
    
    return [callId, timestamp, rawData];
  });
  
  const baseSql = `INSERT IGNORE INTO ${tableName} (
    call_id, timestamp, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} inbound CDR records into ${tableName}`);
    return result;
  } catch (error) {
    console.error('Error batch inserting inbound CDR data:', error);
    throw error;
  }
}

/**
 * Batch insert queue outbound CDR data into raw_outbound table
 * @param {Array<Object>} cdrsData - Array of raw CDR data objects from queue outbound endpoint
 * @param {string} tenant - Tenant key
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawOutbound(cdrsData, tenant) {
  if (!cdrsData || cdrsData.length === 0) {
    return { affectedRows: 0 };
  }

  const tableName = getTenantTables(tenant).rawOutbound;
  
  const batchParams = cdrsData.map(cdr => {
    const callId = cdr.callid || cdr.a_leg || cdr.call_id || null;
    const timestamp = extractRawTimestamp(cdr.timestamp || cdr.called_time) || null;
    const rawData = JSON.stringify(cdr);
    
    return [callId, timestamp, rawData];
  });
  
  const baseSql = `INSERT IGNORE INTO ${tableName} (
    call_id, timestamp, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} outbound CDR records into ${tableName}`);
    return result;
  } catch (error) {
    console.error('Error batch inserting outbound CDR data:', error);
    throw error;
  }
}

/**
 * Batch insert campaign CDR data into raw_campaign table
 * @param {Array<Object>} cdrsData - Array of raw CDR data objects from campaigns endpoint
 * @param {string} tenant - Tenant key
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawCampaign(cdrsData, tenant) {
  if (!cdrsData || cdrsData.length === 0) {
    return { affectedRows: 0 };
  }

  const tableName = getTenantTables(tenant).rawCampaign;
  
  const batchParams = cdrsData.map(cdr => {
    const callId = cdr.callid || cdr.a_leg || cdr.call_id || null;
    const timestamp = extractRawTimestamp(cdr.timestamp || cdr.called_time) || null;
    const rawData = JSON.stringify(cdr);
    
    return [callId, timestamp, rawData];
  });
  
  const baseSql = `INSERT IGNORE INTO ${tableName} (
    call_id, timestamp, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} campaign CDR records into ${tableName}`);
    return result;
  } catch (error) {
    console.error('Error batch inserting campaign CDR data:', error);
    throw error;
  }
}

/**
 * Retrieve raw CDR data from database
 * @param {Object} filters - Filter criteria (startDate, endDate)
 * @param {string} tenant - Tenant key
 * @returns {Promise<Array>} - Array of raw CDR records
 */
export async function getRawCdrsAll(filters = {}, tenant) {
  const tableName = getTenantTables(tenant).rawCdrsAll;
  let sql = `SELECT * FROM ${tableName} WHERE 1=1`;
  const params = [];
  
  const { startDate, endDate } = normalizeDateParams(filters);
  
  if (startDate) {
    const startTs = convertTimestamp(startDate);
    sql += ' AND timestamp >= ?';
    params.push(startTs);
  }
  
  if (endDate) {
    const endTs = convertTimestamp(endDate);
    sql += ' AND timestamp <= ?';
    params.push(endTs);
  }
  
  sql += ' ORDER BY timestamp ASC';
  
  try {
    const rows = await query(sql, params);
    return rows.map(row => ({
      ...row,
      raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
    }));
  } catch (error) {
    console.error('Error retrieving raw CDRs:', error);
    throw error;
  }
}

/**
 * Populate final_report table from raw_cdrs_all data.
 * Extracts: called_time (from agent_history), call_direction, trunk_id (q_gw),
 * callee_id_number, caller_id_number.
 * @param {string} tenant - Tenant key
 * @param {Object} filters - Optional date filters
 * @returns {Promise<Object>} - Result with counts
 */
export async function populateFinalReportFromRaw(tenant, filters = {}) {
  const t = getTenantTables(tenant);
  
  // Fetch all raw records from raw_cdrs_all table only
  console.log(`📊 Fetching CDRs from raw_cdrs_all_${tenant}...`);
  
  const cdrsAllRows = await query(`SELECT * FROM ${t.rawCdrsAll}`);
  
  const rawRecords = cdrsAllRows.map(row => ({
    ...row,
    raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
  }));
  
  console.log(`📊 Processing ${rawRecords.length} raw CDR records from raw_cdrs_all_${tenant}`);
  
  if (rawRecords.length === 0) {
    return { inserted: 0, message: 'No raw CDR records found' };
  }
  
  // Step 1: Group ALL records by bridge_id first
  const bridgeGroups = new Map();
  
  for (const record of rawRecords) {
    const raw = record.raw_data;
    const customChannelVars = raw.custom_channel_vars || {};
    const bridgeId = customChannelVars.bridge_id || raw.call_id;
    
    if (!bridgeGroups.has(bridgeId)) {
      bridgeGroups.set(bridgeId, []);
    }
    bridgeGroups.get(bridgeId).push(raw);
  }
  
  // Step 2: Filter groups that have at least one leg with fonoUC patterns
  const callGroups = new Map();
  let skippedGroups = 0;
  
  for (const [bridgeId, legs] of bridgeGroups) {
    // Check if ANY leg in this group has fonoUC patterns
    const fonoUCLeg = legs.find(leg => {
      const fonoUC = leg.fonoUC || {};
      const ccOutbound = fonoUC.cc_outbound || {};
      const ccCampaign = fonoUC.cc_campaign || {};
      const cc = fonoUC.cc || {};
      
      return (Object.keys(ccOutbound).length > 0 || 
              Object.keys(ccCampaign).length > 0 || 
              Object.keys(cc).length > 0);
    });
    
    // If no leg has fonoUC, skip this entire group
    if (!fonoUCLeg) {
      skippedGroups++;
      continue;
    }
    
    // Include ALL legs of this bridge_id (including ones without fonoUC)
    callGroups.set(bridgeId, legs);
  }
  
  console.log(`📞 Filtered: ${callGroups.size} valid calls with fonoUC patterns, ${skippedGroups} groups skipped`);
  
  // Process each call group and extract data
  const batchParams = [];
  const processedCallIds = new Set();
  
  for (const [callId, legs] of callGroups) {
    // Skip if already processed
    if (processedCallIds.has(callId)) continue;
    processedCallIds.add(callId);
    
    // Find the leg with fonoUC data (should exist due to filtering)
    const fonoUCLeg = legs.find(leg => leg.fonoUC);
    if (!fonoUCLeg) continue; // Safety check
    
    const fonoUC = fonoUCLeg.fonoUC;
    const ccOutbound = fonoUC.cc_outbound || {};
    const ccCampaign = fonoUC.cc_campaign || {};
    const cc = fonoUC.cc || {};
    
    // Determine call direction and get the appropriate cc_* section
    let ccData = null;
    let callDirection = null;
    
    if (Object.keys(ccOutbound).length > 0) {
      ccData = ccOutbound;
      callDirection = 'outbound';
    } else if (Object.keys(ccCampaign).length > 0) {
      ccData = ccCampaign;
      callDirection = 'campaign';
    } else if (Object.keys(cc).length > 0) {
      ccData = cc;
      callDirection = 'inbound';
    }
    
    // Skip if no valid ccData (shouldn't happen due to filtering)
    if (!ccData) continue;
    
    // Extract timestamps ONLY from fonoUC.cc_* section
    // Campaign calls have timestamps in a nested 'timestamps' object
    let calledTime = null;
    let answeredTime = null;
    let hangupTime = null;
    
    if (callDirection === 'campaign') {
      // Campaign calls: timestamps are in ccData.lead.lead_campaign.timestamps
      const lead = ccData.lead || {};
      const leadCampaign = lead.lead_campaign || {};
      const timestamps = leadCampaign.timestamps || {};
      
      calledTime = timestamps.agent_called_time ? Math.floor(timestamps.agent_called_time) : null;
      answeredTime = timestamps.agent_answer_time ? Math.floor(timestamps.agent_answer_time) : null;
      hangupTime = timestamps.agent_hangup_time ? Math.floor(timestamps.agent_hangup_time) : null;
      
      // Fallback to root-level timestamps if not found in fonoUC
      if (!calledTime && fonoUCLeg.channel_created_time) {
        calledTime = Math.floor(fonoUCLeg.channel_created_time / 1000000); // Microseconds to seconds
      }
      if (!calledTime && fonoUCLeg.timestamp) {
        // Gregorian timestamp - convert to Unix
        calledTime = Math.floor(fonoUCLeg.timestamp - 62167219200);
      }
      
      // Calculate answered_time and hangup_time from duration/billing if still not available
      if (calledTime && !answeredTime && fonoUCLeg.duration_seconds && fonoUCLeg.billing_seconds) {
        const durationSec = parseInt(fonoUCLeg.duration_seconds) || 0;
        const billingSec = parseInt(fonoUCLeg.billing_seconds) || 0;
        const ringingTime = durationSec - billingSec;
        answeredTime = calledTime + ringingTime;
        hangupTime = calledTime + durationSec;
      }
    } else {
      // Outbound/Inbound calls: timestamps are at root of ccData
      calledTime = ccData.called_time ? Math.floor(ccData.called_time) : null;
      answeredTime = ccData.answered_time ? Math.floor(ccData.answered_time) : null;
      hangupTime = ccData.hangup_time ? Math.floor(ccData.hangup_time) : null;
    }
    
    // Format timestamps to readable strings (Dubai timezone)
    const formatTs = (ts) => {
      if (!ts) return null;
      const date = new Date(ts * 1000);
      return date.toLocaleString('en-AE', { timeZone: 'Asia/Dubai' });
    };
    
    const calledTimeFormatted = formatTs(calledTime);
    const answeredTimeFormatted = formatTs(answeredTime);
    const hangupTimeFormatted = formatTs(hangupTime);
    
    // Extract trunk_id (q_gw) from custom_sip_headers
    let trunkId = null;
    const customSipHeaders = fonoUCLeg.custom_sip_headers || {};
    trunkId = customSipHeaders.q_gw || null;
    
    // If not found in fonoUC leg, check other legs
    if (!trunkId) {
      const legWithTrunk = legs.find(leg => leg.custom_sip_headers?.q_gw);
      if (legWithTrunk) {
        trunkId = legWithTrunk.custom_sip_headers.q_gw;
      }
    }
    
    // Extract caller and callee numbers from fonoUC leg
    let calleeIdNumber = fonoUCLeg.callee_id_number || null;
    let callerIdNumber = fonoUCLeg.caller_id_number || null;
    
    batchParams.push([
      callId,
      calledTime,
      calledTimeFormatted,
      answeredTime,
      answeredTimeFormatted,
      hangupTime,
      hangupTimeFormatted,
      callDirection,
      trunkId,
      calleeIdNumber,
      callerIdNumber
    ]);
  }
  
  if (batchParams.length === 0) {
    return { inserted: 0, message: 'No valid records to insert' };
  }
  
  console.log(`📝 Prepared ${batchParams.length} consolidated call records for insertion`);
  
  const baseSql = `INSERT IGNORE INTO ${t.finalReport} (
    call_id, called_time, called_time_formatted,
    answered_time, answered_time_formatted,
    hangup_time, hangup_time_formatted,
    call_direction, trunk_id, callee_id_number, caller_id_number
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`✅ Populated ${result.affectedRows} records into ${t.finalReport}`);
    return { inserted: result.affectedRows, total: batchParams.length };
  } catch (error) {
    console.error('Error populating final report:', error);
    throw error;
  }
}

/**
 * Get final report data
 * @param {Object} filters - Filter criteria (startDate, endDate)
 * @param {string} tenant - Tenant key
 * @returns {Promise<Array>} - Array of final report records
 */
export async function getFinalReport(filters = {}, tenant) {
  const tableName = getTenantTables(tenant).finalReport;
  let sql = `SELECT * FROM ${tableName} WHERE 1=1`;
  const params = [];
  
  const { startDate, endDate } = normalizeDateParams(filters);
  
  if (startDate) {
    const startTs = convertTimestamp(startDate);
    sql += ' AND called_time >= ?';
    params.push(startTs);
  }
  
  if (endDate) {
    const endTs = convertTimestamp(endDate);
    sql += ' AND called_time <= ?';
    params.push(endTs);
  }
  
  sql += ' ORDER BY called_time ASC';
  
  try {
    return await query(sql, params);
  } catch (error) {
    console.error('Error retrieving final report:', error);
    throw error;
  }
}


// Check if data exists in database for given date range
export async function checkDataExists(startDate, endDate, tenant) {
  console.log(`🔍 checkDataExists called with: startDate=${startDate}, endDate=${endDate}`);
  const startTs = convertTimestamp(startDate);
  const endTs = convertTimestamp(endDate);
  console.log(`🔍 Converted timestamps: startTs=${startTs}, endTs=${endTs}`);

  const t = getTenantTables(tenant);
  
  try {
    const sql = `SELECT COUNT(*) as count FROM ${t.rawCdrsAll} WHERE timestamp >= ? AND timestamp <= ?`;
    console.log(`🔍 Checking ${t.rawCdrsAll} with query: ${sql} [${startTs}, ${endTs}]`);
    const rows = await query(sql, [startTs, endTs]);
    const count = rows[0].count;
    console.log(`🔍 ${t.rawCdrsAll}: found ${count} records`);
    
    return {
      hasData: count > 0,
      totalRecords: count,
      breakdown: { [t.rawCdrsAll]: count }
    };
  } catch (error) {
    console.error(`❌ Error checking ${t.rawCdrsAll}:`, error);
    return { hasData: false, totalRecords: 0, breakdown: { [t.rawCdrsAll]: 0 } };
  }
}

/**
 * Clear all cached data from database tables
 * @param {Object} options - Options for clearing cache
 * @returns {Promise<Object>} - Result object with cleared counts
 */
export async function clearCache(options = {}, tenant) {
  const t = getTenantTables(tenant);
  const tables = [t.rawCdrsAll];
  const results = {};
  let totalCleared = 0;
  
  console.log('🗑️  Clearing cache memory...');
  
  for (const table of tables) {
    try {
      const countResult = await query(`SELECT COUNT(*) as count FROM ${table}`, []);
      const beforeCount = countResult[0].count;
      
      if (options.specificTable && options.specificTable !== table) {
        results[table] = { before: beforeCount, cleared: 0 };
        continue;
      }
      
      const clearResult = await query(`DELETE FROM ${table}`, []);
      const clearedCount = clearResult.affectedRows || beforeCount;
      
      results[table] = { before: beforeCount, cleared: clearedCount };
      totalCleared += clearedCount;
      
      console.log(`   ✅ ${table}: ${clearedCount} records cleared`);
    } catch (error) {
      console.error(`   ❌ Error clearing ${table}:`, error);
      results[table] = { error: error.message };
    }
  }
  
  console.log(`🧹 Cache cleared: ${totalCleared} total records removed`);
  
  return {
    success: true,
    totalCleared,
    breakdown: results
  };
}

/**
 * Clear specific table cache
 * @param {string} tableName - Name of table to clear
 * @returns {Promise<Object>} - Result object
 */
export async function clearTableCache(tableName, tenant) {
  const t = getTenantTables(tenant);
  const validTables = [t.rawCdrsAll];
  
  if (!validTables.includes(tableName)) {
    throw new Error(`Invalid table name. Valid tables: ${validTables.join(', ')}`);
  }
  
  return await clearCache({ specificTable: tableName }, tenant);
}

/**
 * Filter CDR records by contact number in the database
 * @param {Object} params - Filter parameters including contactNumber, tenant, and date range
 * @returns {Promise<Object>} - Object containing filtered CDR records
 */
async function getRecordsByContactNumber(params = {}) {
  const { contactNumber, startDate, endDate, tenant } = params;
  
  if (!contactNumber || typeof contactNumber !== 'string') {
    throw new Error('Contact number is required for filtering');
  }
  
  const normalizedPhone = normalizePhoneNumber(contactNumber);
  if (!normalizedPhone) {
    throw new Error('Invalid contact number format');
  }
  
  console.log(`🔍 Filtering CDR records by contact number: ${contactNumber} (normalized: ${normalizedPhone})`);
  
  const startTs = startDate ? convertTimestamp(startDate) : null;
  const endTs = endDate ? convertTimestamp(endDate) : null;
  
  const tableName = tenant ? getTenantTables(tenant).rawCdrsAll : 'raw_cdrs_all';
  
  let dateCondition = '';
  if (startTs) dateCondition += ' AND r.timestamp >= ?';
  if (endTs) dateCondition += ' AND r.timestamp <= ?';
  
  const phonePattern = `%${normalizedPhone}%`;
  
  const cdrsQuery = `
    SELECT r.* FROM ${tableName} r
    WHERE (
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.raw_data, '$.caller_id_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.raw_data, '$.callee_id_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.raw_data, '$.to')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.raw_data, '$.from')), '') LIKE ?
    ) ${dateCondition}
    ORDER BY r.timestamp DESC
  `;
  
  const queryParams = [phonePattern, phonePattern, phonePattern, phonePattern];
  if (startTs) queryParams.push(startTs);
  if (endTs) queryParams.push(endTs);
  
  try {
    const records = await query(cdrsQuery, queryParams);
    
    const parsed = records.map(record => ({
      ...record,
      raw_data: typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data
    }));
    
    console.log(`✅ Found ${parsed.length} CDR records matching contact number`);
    
    return {
      cdrRecords: parsed,
      totalRecords: parsed.length
    };
  } catch (error) {
    console.error('❌ Error filtering CDR records by contact number:', error);
    throw error;
  }
}

/**
 * Close all connections in the pool
 * @returns {Promise<void>} - Promise that resolves when all connections are closed
 */
async function end() {
  try {
    console.log('Closing database connection pool...');
    await pool.end();
    console.log('Database connection pool closed successfully');
  } catch (error) {
    console.error('Error closing database connection pool:', error);
    throw error;
  }
}

// Create a default export object with all functions
const dbService = {
  query,
  batchInsert,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  batchInsertRawCdrsAll,
  getRawCdrsAll,
  populateFinalReportFromRaw,
  getFinalReport,
  getRecordsByContactNumber,
  checkDataExists,
  clearCache,
  clearTableCache,
  getTenantSuffix,
  getTenantTables,
  end
};

export default dbService;
