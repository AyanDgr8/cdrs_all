// database.js
// Run this script to create / update the CDR_ALL_db database and all tenant tables.
// Tables are generated dynamically from TENANT_BASE_HEADER in tokenService.js.
//
// Usage:
//   node database.js
//
// To add a new tenant:
//   1. Add the tenant entry to TENANT_BASE_HEADER in tokenService.js
//   2. Re-run: node database.js

import mysql from 'mysql2/promise';
import { TENANT_BASE_HEADER } from './tokenService.js';

const DB_CONFIG = {
  host:  process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'Ayan@1012',
  port: 3306,
  multipleStatements: true
};

const DB_NAME = 'CDR_ALL_db';

// ─── DDL builders ───────────────────────────────────────────────────────────

function createRawCdrsAllTable(suffix) {
  return `
CREATE TABLE IF NOT EXISTS raw_cdrs_all_${suffix} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100),
    timestamp BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_call_id (call_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_timestamp_call_id (timestamp, call_id)
);`;
}

function createFinalReportTable(suffix) {
  return `
CREATE TABLE IF NOT EXISTS final_report_${suffix} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100),
    called_time BIGINT,
    called_time_formatted VARCHAR(50),
    answered_time BIGINT,
    answered_time_formatted VARCHAR(50),
    hangup_time BIGINT,
    hangup_time_formatted VARCHAR(50),
    call_direction VARCHAR(20),
    trunk_id VARCHAR(200),
    callee_id_number VARCHAR(200),
    caller_id_number VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_call_id (call_id),
    INDEX idx_called_time (called_time),
    INDEX idx_call_direction (call_direction),
    INDEX idx_caller_id_number (caller_id_number),
    INDEX idx_callee_id_number (callee_id_number)
);`;
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const conn = await mysql.createConnection(DB_CONFIG);

  try {
    // 1. Create database
    console.log(`\n📦 Ensuring database ${DB_NAME} exists...`);
    await conn.query(
      `CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`
    );
    await conn.query(`USE \`${DB_NAME}\`;`);
    console.log(`✅ Using database: ${DB_NAME}`);

    // 2. Shared users table
    console.log('\n👤 Creating shared tables...');
    await conn.query(`
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    last_login TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`);
    console.log('  ✅ users');

    // Default admin user
    await conn.query(`
INSERT INTO users (username, email, password)
VALUES ('Ayan Khan', 'ayan@multycomm.com', '$2b$10$8XpgD1hs3A5H5hOIGWnp6.lQMJY.xYy9.B9A1iRNJCwCJOY5pMTpO')
ON DUPLICATE KEY UPDATE username = VALUES(username);`);

    // 3. Tenant-specific tables
    const tenants = Object.entries(TENANT_BASE_HEADER);
    console.log(`\n🏢 Found ${tenants.length} tenant(s): ${tenants.map(([k]) => k).join(', ')}`);

    for (const [key, config] of tenants) {
      const suffix = key.toLowerCase().replace(/[^a-z0-9_]/g, '');
      console.log(`\n  📂 Tenant: ${config.name || key} (suffix: _${suffix})`);

      await conn.query(createRawCdrsAllTable(suffix));
      console.log(`    ✅ raw_cdrs_all_${suffix}`);

      await conn.query(createFinalReportTable(suffix));
      console.log(`    ✅ final_report_${suffix}`);
    }

    console.log('\n🎉 All tables created / verified successfully.\n');

  } catch (err) {
    console.error('\n❌ Error setting up database:', err.message);
    process.exit(1);
  } finally {
    await conn.end();
  }
}

main();
