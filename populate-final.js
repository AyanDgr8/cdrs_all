// populate-final.js
// A robust script to populate the database and enhance final_report table
// Usage: node populate-final.js [startTimestamp] [endTimestamp] [tenant]
// Example: node populate-final.js 1693526400 1693612800
// Example with tenant: node populate-final.js 1693526400 1693612800 cogent

/**
 * Combined Database Population and Final Report Enhancement Script
 * 
 * This script combines:
 * 1. populate-db-with-time.js - Populates raw tables from APIs with custom date range
 * 2. populate-final-enhanced.js - Enhances final_report with CDR matching using optimized algorithms
 * 
 * Usage: node populate-final.js [startTimestamp] [endTimestamp] [tenant]
 * Example: node populate-final.js 1693526400 1693612800
 * Example with tenant: node populate-final.js 1693526400 1693612800 cogent
 *
 * If tenant is specified, only that tenant will be processed.
 * If tenant is omitted, all tenants will be processed.
 */

import dotenv from 'dotenv';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import dbService, { populateFinalReportFromRaw } from './dbService.js';
import { TENANT_BASE_HEADER } from './tokenService.js';

dotenv.config();

/**
 * Function to check raw tables for data for a specific tenant
 */
async function checkRawTables(tenant) {
  console.log(`🔍 Checking raw tables for tenant: ${tenant}...`);
  
  const tables = [
    `raw_cdrs_all_${tenant}`
  ];
  
  for (const table of tables) {
    try {
      const result = await dbService.query(`SELECT COUNT(*) as count FROM ${table}`);
      console.log(`📊 ${table}: ${result[0].count} records`);
    } catch (error) {
      console.error(`❌ Error checking ${table}:`, error.message);
    }
  }
}

/**
 * Function to parse command line arguments or use default values
 */
function parseArgs() {
  const args = process.argv.slice(2);
  
  // Default to last 24 hours if no arguments provided
  const endDate = Math.floor(Date.now() / 1000); // Current time in seconds
  const startDate = endDate - (24 * 60 * 60); // 24 hours ago in seconds
  
  let tenant = null;
  
  if (args.length >= 2) {
    const parsedStartDate = parseInt(args[0], 10);
    const parsedEndDate = parseInt(args[1], 10);
    
    if (!isNaN(parsedStartDate) && !isNaN(parsedEndDate)) {
      // Check if third argument is provided (tenant)
      if (args.length >= 3 && args[2].trim()) {
        tenant = args[2].trim().toLowerCase();
        
        // Validate tenant exists in configuration
        if (!TENANT_BASE_HEADER[tenant]) {
          console.error(`❌ Error: Tenant "${tenant}" not found in configuration.`);
          console.log(`Available tenants: ${Object.keys(TENANT_BASE_HEADER).join(', ')}`);
          process.exit(1);
        }
      }
      
      return {
        startDate: parsedStartDate,
        endDate: parsedEndDate,
        tenant
      };
    }
  }
  
  return {
    startDate,
    endDate,
    tenant
  };
}


// Using populateFinalReportFromRaw from dbService.js

/**
 * Main function to fetch data and populate tables for ALL tenants or specific tenant
 */
async function populateDBWithTimeRange() {
  const overallStartTime = Date.now();
  
  try {
    // Parse command line arguments
    const { startDate, endDate, tenant: specificTenant } = parseArgs();
    
    console.log(`📅 Using date range: ${new Date(startDate * 1000).toISOString()} to ${new Date(endDate * 1000).toISOString()}`);
    
    // Determine which tenants to process
    const tenants = specificTenant ? [specificTenant] : Object.keys(TENANT_BASE_HEADER);
    
    if (specificTenant) {
      console.log(`🚀 Starting database population process for SPECIFIC tenant: ${TENANT_BASE_HEADER[specificTenant].name} (${specificTenant})...`);
    } else {
      console.log('🚀 Starting database population process for ALL tenants...');
      console.log(`📋 Tenants to process: ${Object.keys(TENANT_BASE_HEADER).join(', ')}\n`);
    }
    
    const apiParams = {
      start_date: startDate,
      end_date: endDate
    };
    const results = [];
    
    for (const tenant of tenants) {
      console.log(`\n${'='.repeat(80)}`);
      console.log(`🏢 PROCESSING TENANT: ${TENANT_BASE_HEADER[tenant].name} (${tenant})`);
      console.log(`${'='.repeat(80)}\n`);
      
      const tenantStartTime = Date.now();
      
      try {
        // Step 1: Check current state of raw tables
        console.log(`📊 Step 1: Checking current state of raw tables for ${tenant}...`);
        await checkRawTables(tenant);
        
        // Step 2: Fetch data from APIs and populate raw tables
        console.log(`📊 Step 2: Fetching data from APIs and populating raw tables for ${tenant}...`);
        
        // Fetch data from APIs and populate raw tables
        const fetchResults = await fetchAllAPIsAndPopulateDB(tenant, apiParams);
        console.log(`📊 API fetch results for ${tenant}:`, fetchResults);
        
        // Step 3: Check raw tables after population
        console.log(`📊 Step 3: Checking raw tables after population for ${tenant}...`);
        await checkRawTables(tenant);
        
        // Step 4: Populate final_report table with enhanced data
        console.log(`📊 Step 4: Populating final_report table with enhanced data for ${tenant}...`);
        
        // Skip clearing existing data - we want to add to existing records
        console.log(`📊 Adding new data without clearing existing records for ${tenant}...`);
        
        // Use populateFinalReportFromRaw from dbService.js to extract data from raw_cdrs_all
        // and populate final_report table with wanted fields
        const enhancedPopulateResult = await populateFinalReportFromRaw(tenant, {
          startDate,
          endDate
        });
        
        console.log(`✅ Final report population result for ${tenant}:`, enhancedPopulateResult);
        
        // Step 5: Verify final_report table has data
        console.log(`📊 Step 5: Verifying final_report table has data for ${tenant}...`);
        const finalReportCount = await dbService.query(`SELECT COUNT(*) as count FROM final_report_${tenant}`);
        console.log(`📊 final_report_${tenant}: ${finalReportCount[0].count} records`);
        
        // Check call direction distribution
        const distributionQuery = `
          SELECT call_direction, COUNT(*) as count 
          FROM final_report_${tenant}
          GROUP BY call_direction 
          ORDER BY count DESC
        `;
        const distribution = await dbService.query(distributionQuery);
        console.log(`\n📊 Call direction distribution in final_report_${tenant}:`);
        distribution.forEach(row => {
          console.log(`   - ${row.call_direction || 'unknown'}: ${row.count} records`);
        });
        
        const tenantDuration = ((Date.now() - tenantStartTime) / 1000).toFixed(2);
        console.log(`\n✅ Tenant ${tenant} completed successfully in ${tenantDuration}s!`);
        
        results.push({
          tenant,
          success: true,
          duration: tenantDuration,
          recordCount: finalReportCount[0].count
        });
        
      } catch (error) {
        console.error(`❌ Error processing tenant ${tenant}:`, error);
        results.push({
          tenant,
          success: false,
          error: error.message
        });
      }
    }
    
    // Summary
    console.log(`\n\n${'='.repeat(80)}`);
    if (specificTenant) {
      console.log(`📊 POPULATION SUMMARY FOR TENANT: ${specificTenant}`);
    } else {
      console.log('📊 POPULATION SUMMARY FOR ALL TENANTS');
    }
    console.log(`${'='.repeat(80)}\n`);
    
    results.forEach(result => {
      if (result.success) {
        console.log(`✅ ${result.tenant}: ${result.recordCount} records in ${result.duration}s`);
      } else {
        console.log(`❌ ${result.tenant}: FAILED - ${result.error}`);
      }
    });
    
    const overallDuration = ((Date.now() - overallStartTime) / 1000).toFixed(2);
    const successCount = results.filter(r => r.success).length;
    console.log(`\n✅ Overall process completed: ${successCount}/${results.length} tenants successful in ${overallDuration}s!`);
    
  } catch (error) {
    console.error('❌ Error in database population process:', error);
  } finally {
    // Close database connections
    try {
      console.log('\nClosing database connection...');
      await dbService.end();
      console.log('✅ Database connection closed');
    } catch (err) {
      console.error('⚠️ Error closing database connection:', err);
    }
  }
}

// Run the populate function
populateDBWithTimeRange().catch(console.error);
