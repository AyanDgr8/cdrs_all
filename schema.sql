-- ============================================================
-- CDR_ALL_db  –  Shared / static schema
-- ============================================================
-- Tenant-specific tables (raw_cdrs_all_<tenant>,
-- final_report_<tenant>) are created automatically by:
--
--   node database.js
--
-- To add a new tenant:
--   1. Add its entry to TENANT_BASE_HEADER in tokenService.js
--   2. Run: node database.js
-- ============================================================

CREATE DATABASE IF NOT EXISTS CDR_ALL_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE CDR_ALL_db;

-- ────────────────────────────────────────────────────────────
-- Shared table: users (not tenant-specific)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    last_login TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Default admin user (password: Ayan1012)
INSERT INTO users (username, email, password)
VALUES ('Ayan Khan', 'ayan@multycomm.com', '$2b$10$8XpgD1hs3A5H5hOIGWnp6.lQMJY.xYy9.B9A1iRNJCwCJOY5pMTpO')
ON DUPLICATE KEY UPDATE username = VALUES(username);

-- ════════════════════════════════════════════════════════════
-- Tenant-specific tables are NOT defined here.
-- Run  node database.js  to generate them from tokenService.js
-- ════════════════════════════════════════════════════════════
