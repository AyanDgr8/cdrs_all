// server.js

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import https from 'https';
import fs from 'fs';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import { getPortalToken, httpsAgent, TENANT_BASE_HEADER, getTenantConfig } from './tokenService.js';
import requestManager from './requestManager.js';
import jobManager from './jobManager.js';
import axios from 'axios';
import { parseBuffer } from 'music-metadata';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcrypt';
import mysql from 'mysql2/promise';
import dbService, { checkDataExists, clearCache, getTenantTables } from './dbService.js';
import { DateTime } from 'luxon';

dotenv.config();

// Debug: Log environment variables to verify they're loaded correctly
console.log(' Environment variables loaded:');
console.log(`   PORT: ${process.env.PORT}`);
console.log(`   HOST: ${process.env.HOST}`);
console.log(`   PUBLIC_URL: ${process.env.PUBLIC_URL}`);

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Session management middleware - commented out as requested
// app.use((req, res, next) => {
//   // Generate session ID if not exists
//   if (!req.cookies.sessionId) {
//     const sessionId = 'sess_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
//     res.cookie('sessionId', sessionId, { 
//       httpOnly: true, 
//       secure: process.env.NODE_ENV === 'production',
//       maxAge: 30 * 60 * 1000 // 30 minutes
//     });
//     req.sessionId = sessionId;
//     console.log(`🆕 New session created: ${sessionId}`);
//   } else {
//     req.sessionId = req.cookies.sessionId;
//     console.log(`🔄 Existing session: ${req.sessionId}`);
//   }
//   next();
// });

// Add a simple middleware to ensure req.sessionId exists for backward compatibility
// app.use((req, res, next) => {
//   req.sessionId = 'no_session_' + Date.now();
//   next();
// });

const PORT = process.env.PORT || 9898;
const HOST = process.env.HOST || '0.0.0.0'; // 0.0.0.0 ensures the server binds to all network interfaces
const PUBLIC_URL = process.env.PUBLIC_URL || `https://${HOST}:${PORT}`;

console.log(` Server will start on: ${PUBLIC_URL}`);

// Helper to resolve __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, 'public')));

// Route for hot-patch page
app.get('/hot-patch', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'hot-patch.html'));
});

// Tenant-based routing - serve index.html for /:tenant URLs
app.get('/:tenant', (req, res, next) => {
  const tenant = req.params.tenant;
  
  // Skip if it's an API route or static file
  if (tenant.startsWith('api') || tenant.includes('.')) {
    return next();
  }
  
  // Validate tenant exists in configuration
  if (!TENANT_BASE_HEADER[tenant.toLowerCase()]) {
    return res.status(404).send(`Tenant "${tenant}" not found. Available tenants: ${Object.keys(TENANT_BASE_HEADER).join(', ')}`);
  }
  
  // Serve index.html for valid tenant
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// --- Authentication setup ---
const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_key';

// Create MySQL connection pool
const pool = mysql.createPool({
  host:  process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'Ayan@1012',
  database: process.env.DB_NAME || 'CDR_ALL_db',
  port: 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});



// Login
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  try {
    const [rows] = await pool.query(
      'SELECT id, username, email, password FROM users WHERE username = ? OR email = ? LIMIT 1',
      [username, username]
    );
    if (!rows.length) return res.status(401).json({ error: 'Invalid credentials' });
    const user = rows[0];
    const ok = await bcrypt.compare(password, user.password);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });

    await pool.query('UPDATE users SET last_login = NOW() WHERE id = ?', [user.id]);

    const token = jwt.sign({ id: user.id, username: user.username, email: user.email }, JWT_SECRET, { expiresIn: '2h' });
    res.cookie('token', token, { httpOnly: true, sameSite: 'lax', maxAge: 2 * 60 * 60 * 1000 });
    res.json({ success: true, token }); // Include token in response body for iframe scenarios
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Auth check
app.get('/api/auth/check', (req, res) => {
  // Check for token in cookie first
  const cookieToken = req.cookies?.token;
  
  // Then check for Authorization header (Bearer token)
  const authHeader = req.headers.authorization;
  const bearerToken = authHeader && authHeader.startsWith('Bearer ') ? authHeader.substring(7) : null;
  
  // Use either token source
  const token = cookieToken || bearerToken;
  
  if (!token) return res.json({ authenticated: false });
  
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    res.json({ authenticated: true, user: { id: decoded.id, username: decoded.username } });
  } catch {
    res.json({ authenticated: false });
  }
});

// Logout
app.post('/api/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ success: true });
});

// Database population endpoints

// Populate all raw tables with data from all API endpoints
app.post('/api/db/populate', async (req, res) => {
  try {
    const { account, start_date, end_date } = req.body;
    
    if (!account) {
      return res.status(400).json({ error: 'Missing account parameter' });
    }
    
    // Parse dates and convert to epoch milliseconds if provided
    const params = {};
    if (start_date) {
      params.startDate = Date.parse(start_date);
    }
    if (end_date) {
      params.endDate = Date.parse(end_date);
    }
    
    console.log(`🚀 Starting comprehensive database population for account ${account}`, params);
    
    // Use the new comprehensive data fetcher for better performance
    const results = await fetchAndStoreAllDataSequentially(account, params);
    
    res.json({
      success: true,
      message: 'Comprehensive database population completed successfully',
      results,
      summary: {
        totalEndpoints: Object.keys(results).length,
        totalFetched: Object.values(results).reduce((sum, r) => sum + r.fetched, 0),
        totalStored: Object.values(results).reduce((sum, r) => sum + r.stored, 0),
        errors: Object.values(results).filter(r => r.error).length
      }
    });
  } catch (error) {
    console.error('❌ Error in comprehensive database population:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// List all configured tenants
app.get('/api/tenants', (req, res) => {
  const tenants = Object.entries(TENANT_BASE_HEADER).map(([key, cfg]) => ({
    key,
    name: cfg.name,
    domain: cfg.domain
  }));
  res.json({ 
    success: true, 
    tenants
  });
});

// Force populate CDR data
app.post('/api/db/populate/cdrs_all', async (req, res) => {
  try {
    const { tenant, start_date, end_date } = req.body;
    
    if (!tenant) {
      return res.status(400).json({ error: 'Missing tenant parameter' });
    }
    
    const params = {};
    if (start_date) {
      params.startDate = Date.parse(start_date);
    }
    if (end_date) {
      params.endDate = Date.parse(end_date);
    }
    
    console.log(`🎯 Force populating cdrs_all for tenant ${tenant}`, params);
    
    const result = await fetchAllAPIsAndPopulateDB(tenant, params);
    
    res.json({
      success: true,
      message: 'CDR data fetched and stored successfully',
      results: result.results,
      totalTime: result.totalTime,
      params
    });
  } catch (error) {
    console.error(`❌ Error populating cdrs_all:`, error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.post('/api/db/clear', async (req, res) => {
  try {
    const { tenant } = req.body;
    if (!tenant) {
      return res.status(400).json({ success: false, error: 'Tenant parameter is required' });
    }
    const result = await clearCache({}, tenant);
    
    res.json({
      success: true,
      message: 'Cache memory cleared successfully',
      totalCleared: result.totalCleared,
      breakdown: result.breakdown
    });
  } catch (error) {
    console.error('❌ Error clearing cache:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Database statistics endpoint
app.get('/api/db/stats', async (req, res) => {
  try {
    const tenant = req.query.tenant;
    if (!tenant) {
      return res.status(400).json({ success: false, error: 'Tenant parameter is required' });
    }
    const t = getTenantTables(tenant);
    const stats = {};
    
    const [rawCdrsAllCount] = await dbService.query(`SELECT COUNT(*) as count FROM ${t.rawCdrsAll}`);
    const [rawCdrsAllLatest] = await dbService.query(`SELECT MAX(timestamp) as latest FROM ${t.rawCdrsAll}`);
    const [rawCdrsAllEarliest] = await dbService.query(`SELECT MIN(timestamp) as earliest FROM ${t.rawCdrsAll}`);
    const [finalReportCount] = await dbService.query(`SELECT COUNT(*) as count FROM ${t.finalReport}`);
    
    stats.counts = {
      [t.rawCdrsAll]: rawCdrsAllCount.count,
      [t.finalReport]: finalReportCount.count
    };
    
    stats.latest = {
      [t.rawCdrsAll]: rawCdrsAllLatest.latest ? new Date(Number(rawCdrsAllLatest.latest)).toISOString() : null
    };
    
    stats.earliest = {
      [t.rawCdrsAll]: rawCdrsAllEarliest.earliest ? new Date(Number(rawCdrsAllEarliest.earliest)).toISOString() : null
    };
    
    const totalRecords = Object.values(stats.counts).reduce((sum, count) => sum + count, 0);
    
    stats.summary = {
      totalRecords,
      tablesWithData: Object.values(stats.counts).filter(count => count > 0).length,
      isEmpty: totalRecords === 0,
      tenant
    };
    
    res.json(stats);
  } catch (error) {
    console.error('Error getting database statistics:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Job status endpoint
app.get('/api/jobs/:jobId/status', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.getJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json({
    id: job.id,
    status: job.status,
    progress: job.progress,
    startTime: job.startTime,
    endTime: job.endTime,
    error: job.error
  });
});

// Endpoint to manually trigger final report population
app.post('/api/reports/final-report/populate', async (req, res) => {
  const { tenant, startDate, endDate, filters } = req.body;
  
  if (!tenant) {
    return res.status(400).json({ error: 'Tenant parameter is required' });
  }
  
  if (!startDate || !endDate) {
    return res.status(400).json({ error: 'Missing required startDate and endDate parameters' });
  }
  
  try {
    // Generate a unique job ID
    const jobId = `final_report_manual_${Date.now()}`;
    
    // Start a background job to populate the final_report table
    const jobParams = {
      tenant,
      startDate,
      endDate,
      filters
    };
    
    const job = jobManager.startJob(jobId, 'finalReport', jobParams);
    
    res.json({
      success: true,
      message: 'Final report population job started',
      jobId: job.id,
      status: job.status
    });
  } catch (error) {
    console.error('Error starting final report population job:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get job result endpoint
app.get('/api/jobs/:jobId/result', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.getJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  if (job.status === 'running') {
    return res.status(202).json({ 
      message: 'Job still in progress',
      status: job.status,
      progress: job.progress
    });
  }
  
  if (job.status === 'failed') {
    return res.status(500).json({ 
      error: 'Job failed',
      details: job.error
    });
  }
  
  if (job.status === 'completed') {
    return res.json({
      success: true,
      ...job.result
    });
  }
  
  res.status(400).json({ error: 'Invalid job status' });
});

// Cancel job endpoint
app.delete('/api/jobs/:jobId', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.cancelJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json({ 
    message: 'Job cancelled successfully',
    jobId: job.id,
    status: job.status
  });
});

// List all jobs endpoint
app.get('/api/jobs', (req, res) => {
  const jobs = jobManager.getAllJobs().map(job => ({
    id: job.id,
    type: job.type,
    status: job.status,
    progress: job.progress,
    startTime: job.startTime,
    endTime: job.endTime
  }));
  
  res.json({ jobs });
});


// SSL Certificate Management
const loadSSLCertificates = () => {
  try {
    const sslOptions = {
      key: fs.readFileSync('ssl/privkey.pem'),
      cert: fs.readFileSync('ssl/fullchain.pem')
    };
    
    console.log("🔒 SSL certificates loaded successfully");
    return sslOptions;
  } catch (error) {
    console.error("❌ Error loading SSL certificates:", error.message);
    
    // Check if SSL files exist
    const sslFiles = ['ssl/privkey.pem', 'ssl/fullchain.pem'];
    sslFiles.forEach(file => {
      if (!fs.existsSync(file)) {
        console.error(`❌ SSL file not found: ${file}`);
      }
    });
    
    console.log("⚠️  Falling back to HTTP server");
    return null;
  }
};

const sslOptions = loadSSLCertificates();

// Helper: fetch large result sets in chunks to avoid memory pressure
async function processInChunks(sql, values, totalRecords, chunkSize = 5000) {
  const allResults = [];
  let offset = 0;
  while (offset < totalRecords) {
    const chunkSql = `${sql} LIMIT ${chunkSize} OFFSET ${offset}`;
    const chunk = await dbService.query(chunkSql, values);
    allResults.push(...chunk);
    offset += chunkSize;
    if (chunk.length < chunkSize) break;
  }
  return allResults;
}

// Store active queries for progressive loading
const activeQueries = new Map();

// Helper function to generate a unique query ID
function generateQueryId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

// ─── Clustered (server-side) pagination endpoint ───
// Returns ONE page of final_report data at a time with total count + direction summary.
app.post('/api/reports/clustered', async (req, res) => {
  const requestId = Math.random().toString(36).substring(2, 10);

  try {
    const {
      start, end, tenant,
      contact_number, call_direction, trunk_id,
      sort_by = 'called_time', sort_order = 'desc',
      page = 1, pageSize = 100
    } = req.body;

    if (!start || !end) {
      return res.status(400).json({ success: false, message: 'start and end are required', request_id: requestId });
    }

    const finalReportTable = getTenantTables(tenant).finalReport;

    const startEpoch = Math.floor(DateTime.fromISO(start, { zone: 'Asia/Dubai' }).toUTC().toSeconds());
    const endEpochExclusive = Math.floor(DateTime.fromISO(end, { zone: 'Asia/Dubai' }).toUTC().toSeconds());

    // ── Build WHERE clause ──
    let whereClauses = ['called_time >= ?', 'called_time < ?'];
    const whereValues = [startEpoch, endEpochExclusive];

    if (contact_number) {
      whereClauses.push('(caller_id_number LIKE ? OR callee_id_number LIKE ?)');
      whereValues.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (call_direction) { whereClauses.push('call_direction = ?'); whereValues.push(call_direction); }
    if (trunk_id)       { whereClauses.push('trunk_id LIKE ?');    whereValues.push(`%${trunk_id}%`); }

    const whereSQL = whereClauses.join(' AND ');

    // ── 1. Total count ──
    const countSQL = `SELECT COUNT(*) as total FROM ${finalReportTable} USE INDEX (idx_called_time) WHERE ${whereSQL}`;
    const countResult = await dbService.query(countSQL, whereValues);
    const totalRecords = countResult[0].total;

    // ── 2. Direction summary ──
    const summarySQL = `SELECT call_direction, COUNT(*) as cnt FROM ${finalReportTable} USE INDEX (idx_called_time) WHERE ${whereSQL} GROUP BY call_direction`;
    const summaryRows = await dbService.query(summarySQL, whereValues);
    const summary = {};
    summaryRows.forEach(r => { summary[r.call_direction || 'unknown'] = r.cnt; });

    // ── 3. Validate sort column ──
    const validSortColumns = [
      'call_id', 'called_time', 'called_time_formatted',
      'answered_time', 'hangup_time',
      'call_direction', 'trunk_id', 'callee_id_number', 'caller_id_number',
      'created_at', 'updated_at'
    ];
    const sortColumn = validSortColumns.includes(sort_by) ? sort_by : 'called_time';
    const sortDir = sort_order?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    // ── 4. Paginated data query ──
    const safePageSize = Math.min(Math.max(parseInt(pageSize) || 100, 10), 500);
    const safePage = Math.max(parseInt(page) || 1, 1);
    const offset = (safePage - 1) * safePageSize;
    const totalPages = Math.ceil(totalRecords / safePageSize);

    const dataSQL = `SELECT call_id, called_time, called_time_formatted,
      answered_time, answered_time_formatted, hangup_time, hangup_time_formatted,
      call_direction, trunk_id, callee_id_number, caller_id_number, created_at
      FROM ${finalReportTable} USE INDEX (idx_called_time)
      WHERE ${whereSQL}
      ORDER BY ${sortColumn} ${sortDir}
      LIMIT ${safePageSize} OFFSET ${offset}`;

    const data = await dbService.query(dataSQL, whereValues);

    return res.json({
      success: true,
      data,
      page: safePage,
      pageSize: safePageSize,
      totalPages,
      totalRecords,
      summary,
      request_id: requestId
    });

  } catch (error) {
    console.error(`Clustered query error (${requestId}):`, error.message);
    return res.status(500).json({ success: false, error: error.message, request_id: requestId });
  }
});

// Progressive loading endpoint
app.get('/api/reports/progressive', async (req, res) => {
  const { queryId, page = '1' } = req.query;
  const pageNum = parseInt(page) || 1;
  const pageSize = 1000; // Page size set to 1000 for consistent batch loading
  const debugHtmlContent = true; // Define debugHtmlContent here
  try {
    // If no queryId provided, this is a new query request
    if (!queryId) {
      return res.status(400).json({
        success: false,
        error: 'Missing queryId parameter'
      });
    }
    
    // Check if this is an active query
    if (!activeQueries.has(queryId)) {
      return res.status(404).json({
        success: false,
        error: 'Query not found or expired'
      });
    }
    
    const queryData = activeQueries.get(queryId);
    const { sql, values, totalRecords } = queryData;
    
    // Calculate offset based on page number
    const offset = (pageNum - 1) * pageSize;
    
    // Check if we're requesting beyond available records
    if (offset >= totalRecords) {
      return res.json({
        success: true,
        data: [],
        page: pageNum,
        totalPages: Math.ceil(totalRecords / pageSize),
        totalRecords,
        isLastPage: true
      });
    }
    
    // Execute query for this page
    // Fix any potential SQL syntax issues with the ORDER BY clause
    let fixedSql = sql;
    
    // Check if there's a duplicate transfer_event filter after ORDER BY
    const orderByIndex = fixedSql.indexOf('ORDER BY');
    if (orderByIndex > 0) {
      // Split the query into parts before and after ORDER BY
      const beforeOrderBy = fixedSql.substring(0, orderByIndex).trim();
      let orderByClause = fixedSql.substring(orderByIndex);
      
      // Check if there's an AND condition after ORDER BY (which is invalid SQL)
      const andAfterOrderBy = orderByClause.indexOf('AND');
      if (andAfterOrderBy > 0) {
        // Remove everything after AND in the ORDER BY clause
        orderByClause = orderByClause.substring(0, andAfterOrderBy).trim();
        fixedSql = beforeOrderBy + ' ' + orderByClause;
      }
    }
    
    // Ensure the SQL query doesn't have any trailing AND or WHERE
    if (fixedSql.trim().endsWith('WHERE')) {
      fixedSql = fixedSql.trim().slice(0, -5); // Remove the trailing 'WHERE'
    }
    if (fixedSql.trim().endsWith('AND')) {
      fixedSql = fixedSql.trim().slice(0, -3); // Remove the trailing 'AND'
    }
    
    const pageSql = `${fixedSql} LIMIT ${pageSize} OFFSET ${offset}`;
    
    const results = await dbService.query(pageSql, values);
    
    // Debug: Log the first result to see if hold_duration_intervals is included
    console.log(`Total records matching query: ${totalRecords}`);
    console.log('First result keys:', Object.keys(results[0] || {}));
    console.log('hold_duration_intervals in first result:', results[0]?.hold_duration_intervals);
        
    // Debug: Show first few records' timestamp and formatted time data
    if (results.length > 0) {
      console.log('=== DEBUG: First 3 records timestamp data ===');
      results.slice(0, 3).forEach((record, index) => {
        console.log(`Record ${index + 1}:`);
        console.log(`  called_time: ${record.called_time}`);
        console.log(`  called_time_formatted: ${record.called_time_formatted}`);
      });
      console.log('=== END DEBUG ===');
    }
    
    // Calculate if this is the last page
    const totalPages = Math.ceil(totalRecords / pageSize);
    const isLastPage = pageNum >= totalPages;
    
    // If this is the last page, clean up the query data
    if (isLastPage) {
      activeQueries.delete(queryId);
    }
    
    return res.json({
      success: true,
      data: results,
      page: pageNum,
      totalPages,
      totalRecords,
      isLastPage
    });
    
  } catch (error) {
    console.error(`Error processing progressive query: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Error processing query',
      details: error.message
    });
  }
});

// Initialize a progressive query
app.post('/api/reports/progressive/init', async (req, res) => {
  const requestId = Math.random().toString(36).substring(2, 10);
  
  try {
    const {
      start, end, tenant,
      contact_number, call_direction, trunk_id,
      sort_by = 'called_time',
      sort_order = 'desc'
    } = req.body;

    const progressFinalReportTable = getTenantTables(tenant).finalReport;
    
    if (!start || !end) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: start and end dates are required',
        request_id: requestId
      });
    }

    const startEpoch = Math.floor(
      DateTime.fromISO(start, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );
    const endEpochExclusive = Math.floor(
      DateTime.fromISO(end, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );
    
    let sql = `SELECT call_id, called_time, called_time_formatted, call_direction, trunk_id, callee_id_number, caller_id_number, created_at FROM ${progressFinalReportTable} USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    const values = [startEpoch, endEpochExclusive];

    if (contact_number) {
      sql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      values.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (call_direction) {
      sql += ' AND call_direction = ?';
      values.push(call_direction);
    }
    if (trunk_id) {
      sql += ' AND trunk_id LIKE ?';
      values.push(`%${trunk_id}%`);
    }
    
    const validSortColumns = [
      'call_id', 'called_time', 'called_time_formatted', 'call_direction',
      'trunk_id', 'callee_id_number', 'caller_id_number', 'created_at', 'updated_at'
    ];
    const sortColumn = validSortColumns.includes(sort_by) ? sort_by : 'called_time';
    const sortDir = sort_order?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    sql += ` ORDER BY ${sortColumn} ${sortDir}`;
    
    // Count query
    let countSql = `SELECT COUNT(*) as total FROM ${progressFinalReportTable} USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    let countValues = [startEpoch, endEpochExclusive];
    
    if (contact_number) {
      countSql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      countValues.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (call_direction) {
      countSql += ' AND call_direction = ?';
      countValues.push(call_direction);
    }
    if (trunk_id) {
      countSql += ' AND trunk_id LIKE ?';
      countValues.push(`%${trunk_id}%`);
    }
    
    const countResult = await dbService.query(countSql, countValues);
    const totalRecords = countResult[0].total;
    
    console.log(`Total records matching query: ${totalRecords}`);
    
    const queryId = generateQueryId();
    
    activeQueries.set(queryId, {
      sql,
      values,
      totalRecords,
      createdAt: Date.now(),
      lastAccessed: Date.now()
    });
    
    setTimeout(() => {
      if (activeQueries.has(queryId)) {
        console.log(`Cleaning up expired query ${queryId}`);
        activeQueries.delete(queryId);
      }
    }, 30 * 60 * 1000);
    
    return res.json({
      success: true,
      queryId,
      totalRecords,
      totalPages: Math.ceil(totalRecords / 1000),
      message: 'Query initialized successfully. Use this queryId to fetch pages of results.',
      request_id: requestId
    });
    
  } catch (error) {
    console.error(`Error initializing progressive query: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Error initializing query',
      details: error.message,
      request_id: requestId
    });
  }
});

app.get('/api/filters/call-direction', async (req, res) => {
  try {
    const { tenant } = req.query;

    if (!tenant) {
      return res.status(400).json({ success: false, error: 'tenant parameter is required' });
    }

    const filterTable = getTenantTables(tenant).finalReport;
    const sql = `
      SELECT DISTINCT call_direction
      FROM ${filterTable}
      WHERE call_direction IS NOT NULL AND call_direction != ''
      ORDER BY call_direction
    `;

    const rows = await dbService.query(sql);
    res.json({ success: true, data: rows.map(r => r.call_direction) });

  } catch (err) {
    console.error('Call direction filter error:', err);
    res.status(500).json({ success: false, error: 'Failed to fetch call direction list' });
  }
});

app.get('/api/filters/trunk-id', async (req, res) => {
  try {
    const { tenant } = req.query;

    if (!tenant) {
      return res.status(400).json({ success: false, error: 'tenant parameter is required' });
    }

    const filterTable = getTenantTables(tenant).finalReport;
    const sql = `
      SELECT DISTINCT trunk_id
      FROM ${filterTable}
      WHERE trunk_id IS NOT NULL AND trunk_id != ''
      ORDER BY trunk_id
    `;

    const rows = await dbService.query(sql);
    res.json({ success: true, data: rows.map(r => r.trunk_id) });

  } catch (err) {
    console.error('Trunk ID filter error:', err);
    res.status(500).json({ success: false, error: 'Failed to fetch trunk ID list' });
  }
});


app.get('/api/reports/search', async (req, res) => {
  const requestId = Math.random().toString(36).substring(2, 10);
  
  try {
    const {
      start, end, tenant,
      contact_number, call_direction, trunk_id,
      sort_by = 'called_time',
      sort_order = 'desc',
      fetchAll = 'false'
    } = req.query;

    const searchFinalReportTable = getTenantTables(tenant).finalReport;
    
    if (!start || !end) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: start and end dates are required',
        request_id: requestId
      });
    }

    const startEpoch = Math.floor(
      DateTime.fromISO(start, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );
    const endEpochExclusive = Math.floor(
      DateTime.fromISO(end, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );

    let sql = `SELECT call_id, called_time, called_time_formatted, answered_time, answered_time_formatted, hangup_time, hangup_time_formatted, call_direction, trunk_id, callee_id_number, caller_id_number, created_at FROM ${searchFinalReportTable} USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    const values = [startEpoch, endEpochExclusive];
    
    if (contact_number) {
      sql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      values.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (call_direction) {
      sql += ' AND call_direction = ?';
      values.push(call_direction);
    }
    if (trunk_id) {
      sql += ' AND trunk_id LIKE ?';
      values.push(`%${trunk_id}%`);
    }
    
    const validSortColumns = [
      'call_id', 'called_time', 'called_time_formatted',
      'answered_time', 'hangup_time',
      'call_direction', 'trunk_id', 'callee_id_number', 'caller_id_number',
      'created_at', 'updated_at'
    ];
    const sortColumn = validSortColumns.includes(sort_by) ? sort_by : 'called_time';
    const sortDir = sort_order?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    sql += ` ORDER BY ${sortColumn} ${sortDir}`;

    const shouldFetchAll = fetchAll === 'true';
    
    // Count query
    let countSql = `SELECT COUNT(*) as total FROM ${searchFinalReportTable} USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    let countValues = [startEpoch, endEpochExclusive];
    
    if (contact_number) {
      countSql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      countValues.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (call_direction) {
      countSql += ' AND call_direction = ?';
      countValues.push(call_direction);
    }
    if (trunk_id) {
      countSql += ' AND trunk_id LIKE ?';
      countValues.push(`%${trunk_id}%`);
    }
    
    const countResult = await dbService.query(countSql, countValues);
    const totalRecords = countResult[0].total;
    
    const queryStartTime = Date.now();
    const timeoutDuration = shouldFetchAll ? 1200000 : 180000;
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Query timeout exceeded')), timeoutDuration);
    });
    
    let results;
    try {
      if (shouldFetchAll) {
        results = await Promise.race([
          processInChunks(sql, values, totalRecords, 5000),
          timeoutPromise
        ]);
      } else {
        const limitedSql = `${sql} LIMIT 50000`;
        results = await Promise.race([
          dbService.query(limitedSql, values),
          timeoutPromise
        ]);
      }
    } catch (error) {
      console.error(`❌ Query error: ${error.message}`);
      return res.status(504).json({ error: 'Query timeout or database error', details: error.message });
    }
    
    const queryDuration = Date.now() - queryStartTime;
    
    // Direction summary
    const totals = { Total: totalRecords };
    results.forEach(record => {
      const dir = record.call_direction || 'unknown';
      totals[dir] = (totals[dir] || 0) + 1;
    });
    
    return res.json({
      success: true,
      data: results,
      totals,
      fetchAll: shouldFetchAll,
      query_time_ms: queryDuration,
      request_id: requestId
    });
  } catch (error) {
    let statusCode = 500;
    let errorMessage = 'Internal server error';
    let errorDetails = null;
    
    if (error.message && error.message.includes('Missing required parameter')) {
      statusCode = 400;
      errorMessage = error.message;
    } else if (error.message && (error.message.includes('timeout') || error.message.includes('Query timeout'))) {
      statusCode = 504;
      errorMessage = 'Query timed out. Please try with a smaller date range or more specific filters.';
    } else if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.code === 'EPIPE' || error.code === 'EMFILE') {
      statusCode = 503;
      errorMessage = `Database connection error (${error.code}). Please try again in a few moments.`;
      errorDetails = { code: error.code };
    } else if (error.code && error.code.startsWith('ER_')) {
      statusCode = 500;
      errorMessage = 'Database error. Please check your query parameters.';
      errorDetails = { code: error.code };
    }
    
    res.status(statusCode).json({ 
      error: errorMessage,
      request_id: requestId,
      ...(errorDetails && { details: errorDetails })
    });
  }
});

// Only use HTTPS if PUBLIC_URL starts with https://
const useHTTPS = PUBLIC_URL.startsWith('https://');

if (sslOptions && useHTTPS) {
  const server = https.createServer(sslOptions, app);
  server.listen(PORT, HOST, () => {
    console.log(`🔐 HTTPS server running at ${PUBLIC_URL}`);
    console.log(`🌐 Server accessible on all network interfaces (${HOST}:${PORT})`);
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTPS Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
} else {
  const server = app.listen(PORT, HOST, () => {
    console.log(`🌐 HTTP server running at ${PUBLIC_URL}`);
    if (!useHTTPS) {
      console.log(`⚠️  Running in HTTP mode (PUBLIC_URL is set to HTTP)`);
    } else {
      console.log(`⚠️  Running in HTTP mode (no SSL certificates found)`);
    }
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTP Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
}

