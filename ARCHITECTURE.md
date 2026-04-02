# CDR Processing Architecture

## Overview
Simplified architecture: **One API → One Raw Table → One Final Table**

## Data Flow

### 1. API Fetch (`apiDataFetcher.js`)
- **Endpoint**: `/api/v2/reports/cdrs/all`
- **Pagination**: Automatic (up to 25,000 pages, 2000 records/page)
- **Storage**: All CDRs stored in `raw_cdrs_all_${tenant}`

### 2. Raw Storage (`raw_cdrs_all_${tenant}`)
- Stores complete JSON from API
- No classification or filtering at this stage
- All CDR types stored together

### 3. Final Report Extraction (`dbService.js` → `populateFinalReportFromRaw`)

#### Filtering Logic
**ONLY processes CDRs with these fonoUC patterns:**

**Outbound:**
```json
"fonoUC": {
  "cc_outbound": { ... }
}
```

**Campaign:**
```json
"fonoUC": {
  "cc_campaign": { ... }
}
```

**Inbound:**
```json
"fonoUC": {
  "cc": { ... }
}
```

**All other CDRs are SKIPPED.**

#### Data Extraction

**From `fonoUC.cc_outbound` / `cc_campaign` / `cc`:**
- `call_id` → from `agent_callid` or `callid`
- `called_time` → from `called_time` (Unix timestamp)
- `answered_time` → from `answered_time` (Unix timestamp)
- `hangup_time` → from `hangup_time` (Unix timestamp)
- `call_direction` → determined by which section exists (outbound/campaign/inbound)

**From `custom_sip_headers`:**
- `trunk_id` → from `q_gw`

**From root level:**
- `caller_id_number`
- `callee_id_number`

### 4. Final Report Table (`final_report_${tenant}`)

**Schema:**
```sql
- id (AUTO_INCREMENT PRIMARY KEY)
- call_id (VARCHAR(100), UNIQUE)
- called_time (BIGINT)
- called_time_formatted (VARCHAR(50)) -- Dubai timezone
- answered_time (BIGINT)
- answered_time_formatted (VARCHAR(50)) -- Dubai timezone
- hangup_time (BIGINT)
- hangup_time_formatted (VARCHAR(50)) -- Dubai timezone
- call_direction (VARCHAR(20)) -- 'outbound', 'campaign', 'inbound'
- trunk_id (VARCHAR(200)) -- from q_gw
- callee_id_number (VARCHAR(200))
- caller_id_number (VARCHAR(200))
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
```

## Example: Outbound Call Processing

**Input JSON (from your example):**
```json
{
  "call_id": "9nio3nppns9aeav4mvj5",
  "custom_sip_headers": {
    "q_gw": "trunk_123"
  },
  "caller_id_number": "+97147777333",
  "callee_id_number": "4915752210172",
  "fonoUC": {
    "cc_outbound": {
      "called_time": 1775068736.4498415,
      "answered_time": 1775068745.750376,
      "hangup_time": 1775068762.9555037,
      "agent_callid": "9nio3nppns9aeav4mvj5"
    }
  }
}
```

**Output in final_report_meydan:**
```
call_id: 9nio3nppns9aeav4mvj5
called_time: 1775068736
called_time_formatted: 01/04/2026, 12:32:16 (Dubai)
answered_time: 1775068745
answered_time_formatted: 01/04/2026, 12:32:25 (Dubai)
hangup_time: 1775068762
hangup_time_formatted: 01/04/2026, 12:32:42 (Dubai)
call_direction: outbound
trunk_id: trunk_123
caller_id_number: +97147777333
callee_id_number: 4915752210172
```

## Command Execution

```bash
node populate-final.js 1774987200 1775073540
```

**Date Range:**
- Start: 01/04/2026, 12:00AM (1774987200)
- End: 01/04/2026, 11:59PM (1775073540)

**Process:**
1. Fetch all CDRs from API for date range
2. Store in `raw_cdrs_all_meydan`
3. Filter for fonoUC patterns only
4. Extract specified fields
5. Insert into `final_report_meydan`

## Key Features

✅ **Simplified**: One API, one raw table, one final table
✅ **Filtered**: Only processes CDRs with fonoUC.cc_outbound/cc_campaign/cc
✅ **Timestamps**: Extracted from fonoUC sections (not root level)
✅ **Trunk ID**: Extracted from custom_sip_headers.q_gw
✅ **Dubai Timezone**: All formatted timestamps in Asia/Dubai
✅ **Duplicate Prevention**: UNIQUE constraint on call_id
