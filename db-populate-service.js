// db-populate-service.js
// A robust service script to automatically populate the database at regular intervals.
// Simplified for the cdrs_all schema: fetches from /api/v2/reports/cdrs/all,
// stores raw data, then populates the final_report table.
//
// Usage: node db-populate-service.js [intervalMinutes] [lookbackHours]
// Example: node db-populate-service.js 5 1

import dotenv from 'dotenv';
dotenv.config();

import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import dbService, { getTenantTables, populateFinalReportFromRaw } from './dbService.js';
import { TENANT_BASE_HEADER } from './tokenService.js';
import fs from 'fs';
import path from 'path';

// Get first tenant from configuration
const DEFAULT_TENANT = Object.keys(TENANT_BASE_HEADER)[0];
if (!DEFAULT_TENANT) {
  console.error('❌ No tenants configured in TENANT_BASE_HEADER');
  process.exit(1);
}

// Default configuration
const DEFAULT_INTERVAL_MINUTES = 5;
const DEFAULT_LOOKBACK_HOURS = 1;
const LOG_FILE = path.join(process.cwd(), 'db-populate-service.log');

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const intervalMinutes = args.length >= 1 ? parseInt(args[0], 10) : DEFAULT_INTERVAL_MINUTES;
  const lookbackHours = args.length >= 2 ? parseInt(args[1], 10) : DEFAULT_LOOKBACK_HOURS;
  
  return {
    intervalMinutes: isNaN(intervalMinutes) ? DEFAULT_INTERVAL_MINUTES : intervalMinutes,
    lookbackHours: isNaN(lookbackHours) ? DEFAULT_LOOKBACK_HOURS : lookbackHours
  };
}

// Custom logging function that writes to both console and log file
function log(message, type = 'info') {
  const timestamp = new Date().toISOString();
  let formattedMessage;
  
  switch (type) {
    case 'error':
      formattedMessage = `[${timestamp}] ❌ ERROR: ${message}`;
      console.error(formattedMessage);
      break;
    case 'warning':
      formattedMessage = `[${timestamp}] ⚠️ WARNING: ${message}`;
      console.warn(formattedMessage);
      break;
    case 'success':
      formattedMessage = `[${timestamp}] ✅ SUCCESS: ${message}`;
      console.log(formattedMessage);
      break;
    default:
      formattedMessage = `[${timestamp}] ℹ️ INFO: ${message}`;
      console.log(formattedMessage);
  }
  
  // Append to log file
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
  return formattedMessage;
}

// Main function to fetch data and populate tables
async function populateDBWithTimeRange() {
  const startTime = Date.now();
  const runId = `run-${Date.now()}`;
  log(`Starting database population (ID: ${runId})...`);
  
  try {
    // Calculate time range
    const endDate = Math.floor(Date.now() / 1000);
    const startDate = endDate - (config.lookbackHours * 60 * 60);
    
    log(`Date range: ${new Date(startDate * 1000).toISOString()} → ${new Date(endDate * 1000).toISOString()} (${config.lookbackHours}h lookback)`);
    
    const t = getTenantTables(DEFAULT_TENANT);
    
    // Step 1: Count raw records before fetch
    log('Step 1: Checking raw table before fetch...');
    const beforeResult = await dbService.query(`SELECT COUNT(*) as count FROM ${t.rawCdrsAll}`);
    const beforeCount = beforeResult[0].count;
    log(`${t.rawCdrsAll}: ${beforeCount} records`);
    
    // Step 2: Fetch from API → raw_cdrs_all table
    log('Step 2: Fetching CDRs from API...');
    const fetchResults = await fetchAllAPIsAndPopulateDB(DEFAULT_TENANT, {
      start_date: startDate,
      end_date: endDate
    });
    log(`API fetch results: ${JSON.stringify(fetchResults)}`);
    
    // Step 3: Count raw records after fetch
    log('Step 3: Checking raw table after fetch...');
    const afterResult = await dbService.query(`SELECT COUNT(*) as count FROM ${t.rawCdrsAll}`);
    const afterCount = afterResult[0].count;
    const newRawRecords = afterCount - beforeCount;
    log(`${t.rawCdrsAll}: ${afterCount} records (+${newRawRecords} new)`);
    
    // Step 4: Populate final_report from raw data
    log('Step 4: Populating final_report from raw data...');
    const populateResult = await populateFinalReportFromRaw(DEFAULT_TENANT, {
      startDate,
      endDate
    });
    log(`Final report populated: ${JSON.stringify(populateResult)}`, 'success');
    
    // Step 5: Verify final_report
    log('Step 5: Verifying final_report...');
    const finalCount = await dbService.query(`SELECT COUNT(*) as count FROM ${t.finalReport}`);
    log(`${t.finalReport}: ${finalCount[0].count} total records`);
    
    // Direction distribution
    const distQuery = `
      SELECT call_direction, COUNT(*) as count 
      FROM ${t.finalReport} 
      GROUP BY call_direction 
      ORDER BY count DESC
    `;
    const distribution = await dbService.query(distQuery);
    log('Call direction distribution:');
    distribution.forEach(row => {
      log(`  ${row.call_direction || '(empty)'}: ${row.count} records`);
    });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    log(`Process completed in ${duration}s`, 'success');
    
    updateStatusFile({
      lastRun: new Date().toISOString(),
      status: 'success',
      duration: `${duration}s`,
      newRawRecords,
      finalReportInserted: populateResult.inserted || 0,
      nextRun: new Date(Date.now() + (config.intervalMinutes * 60 * 1000)).toISOString()
    });
    
    return true;
  } catch (error) {
    log(`Error in population process (ID: ${runId}): ${error.message}`, 'error');
    log(error.stack, 'error');
    
    updateStatusFile({
      lastRun: new Date().toISOString(),
      status: 'error',
      error: error.message,
      nextRun: new Date(Date.now() + (config.intervalMinutes * 60 * 1000)).toISOString()
    });
    
    return false;
  }
}

// Function to update status file
function updateStatusFile(status) {
  const statusFile = path.join(process.cwd(), 'db-populate-status.json');
  
  try {
    // Read existing status if available
    let currentStatus = {};
    if (fs.existsSync(statusFile)) {
      try {
        const fileContent = fs.readFileSync(statusFile, 'utf8');
        currentStatus = JSON.parse(fileContent);
      } catch (parseError) {
        // File is corrupted, reset it
        log(`Status file corrupted, resetting: ${parseError.message}`, 'warn');
        currentStatus = {};
      }
    }
    
    // Update with new status
    const updatedStatus = {
      ...currentStatus,
      ...status,
      lastUpdated: new Date().toISOString()
    };
    
    // Write updated status
    fs.writeFileSync(statusFile, JSON.stringify(updatedStatus, null, 2));
  } catch (error) {
    log(`Error updating status file: ${error.message}`, 'error');
  }
}

// Function to run the service with error handling and recovery
async function runService() {
  try {
    await populateDBWithTimeRange();
  } catch (error) {
    log(`Critical service error: ${error.message}`, 'error');
    log(error.stack, 'error');
  }
  
  // Schedule next run regardless of success or failure
  scheduleNextRun();
}

// Function to schedule the next run
function scheduleNextRun() {
  const intervalMs = config.intervalMinutes * 60 * 1000;
  const nextRunTime = new Date(Date.now() + intervalMs);
  log(`Scheduling next run at ${nextRunTime.toISOString()} (in ${config.intervalMinutes} minutes)`);
  
  // Update status file with next run time
  updateStatusFile({
    nextRun: nextRunTime.toISOString()
  });
  
  setTimeout(() => {
    runService();
  }, intervalMs);
}

// Initialize log file
function initializeLogFile() {
  const header = `
=========================================
DB POPULATE SERVICE STARTED
=========================================
Date: ${new Date().toISOString()}
Interval: ${config.intervalMinutes} minutes
Lookback: ${config.lookbackHours} hours
=========================================
`;
  
  // Create or truncate log file
  fs.writeFileSync(LOG_FILE, header);
  log('Log file initialized');
}

// Parse configuration from command line arguments
const config = parseArgs();
log(`Service configured with: ${config.intervalMinutes} minute intervals, ${config.lookbackHours} hour lookback`);

// Initialize
(async () => {
  // Initialize log file
  initializeLogFile();
  
  // Create initial status file
  updateStatusFile({
    serviceStarted: new Date().toISOString(),
    status: 'starting',
    config: {
      intervalMinutes: config.intervalMinutes,
      lookbackHours: config.lookbackHours
    }
  });
  
  // Start the service
  log('Starting initial run...');
  await runService();
})();

// Handle graceful shutdown
process.on('SIGINT', () => {
  log('Received SIGINT. Shutting down gracefully...', 'warning');
  updateStatusFile({
    status: 'stopped',
    reason: 'SIGINT received'
  });
  process.exit(0);
});

process.on('SIGTERM', () => {
  log('Received SIGTERM. Shutting down gracefully...', 'warning');
  updateStatusFile({
    status: 'stopped',
    reason: 'SIGTERM received'
  });
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  log(`Uncaught exception: ${error.message}`, 'error');
  log(error.stack, 'error');
  updateStatusFile({
    status: 'crashed',
    error: error.message,
    stack: error.stack
  });
  
  // Give time for logs to be written before exiting
  setTimeout(() => {
    process.exit(1);
  }, 1000);
});
