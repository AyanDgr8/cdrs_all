// repopulate-final.js
// Re-populate final_report table from existing raw_cdrs_all data
// Usage: node repopulate-final.js [tenant]

import dotenv from 'dotenv';
import dbService, { populateFinalReportFromRaw } from './dbService.js';
import { TENANT_BASE_HEADER } from './tokenService.js';

dotenv.config();

async function repopulateFinalReport() {
  try {
    // Get tenant from command line or use default
    const tenant = process.argv[2] || 'meydan';
    
    // Validate tenant
    if (!TENANT_BASE_HEADER[tenant]) {
      console.error(`❌ Invalid tenant: ${tenant}`);
      console.log(`Available tenants: ${Object.keys(TENANT_BASE_HEADER).join(', ')}`);
      process.exit(1);
    }
    
    console.log(`\n🔄 Re-populating final_report_${tenant} from raw_cdrs_all_${tenant}...\n`);
    
    // Clear existing data
    console.log(`🗑️  Clearing existing data from final_report_${tenant}...`);
    await dbService.query(`TRUNCATE TABLE final_report_${tenant}`);
    console.log(`✅ Cleared\n`);
    
    // Re-populate from raw data
    console.log(`📊 Extracting and populating from raw_cdrs_all_${tenant}...`);
    const result = await populateFinalReportFromRaw(tenant);
    
    console.log(`\n✅ Re-population complete!`);
    console.log(`📊 Result:`, result);
    
    // Verify trunk_id extraction
    console.log(`\n🔍 Verifying trunk_id extraction...`);
    const trunkStats = await dbService.query(`
      SELECT 
        trunk_id,
        COUNT(*) as count 
      FROM final_report_${tenant} 
      WHERE trunk_id IS NOT NULL AND trunk_id != ''
      GROUP BY trunk_id 
      ORDER BY count DESC 
      LIMIT 10
    `);
    
    if (trunkStats.length > 0) {
      console.log(`✅ Found ${trunkStats.length} different trunk_id values:`);
      trunkStats.forEach(row => {
        console.log(`   - ${row.trunk_id}: ${row.count} records`);
      });
    } else {
      console.log(`⚠️  No trunk_id values found`);
    }
    
    // Check total records
    const totalCheck = await dbService.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(trunk_id) as with_trunk,
        COUNT(CASE WHEN trunk_id IS NOT NULL AND trunk_id != '' THEN 1 END) as with_trunk_value
      FROM final_report_${tenant}
    `);
    
    console.log(`\n📊 Final Statistics:`);
    console.log(`   Total records: ${totalCheck[0].total}`);
    console.log(`   With trunk_id (not null): ${totalCheck[0].with_trunk}`);
    console.log(`   With trunk_id (has value): ${totalCheck[0].with_trunk_value}`);
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await dbService.end();
    console.log('\n✅ Done\n');
  }
}

repopulateFinalReport();
