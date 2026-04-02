// final-report.js

// Timezone constants
const TZ_UTC = 'UTC';
const TZ_DUBAI = 'Asia/Dubai';
const TZ_INDIA = 'Asia/Kolkata';

// Returns the currently selected IANA timezone string
function getSelectedTimezone() {
  const sel = document.getElementById('timezoneSelect');
  return sel ? sel.value : TZ_DUBAI;
}

// Returns the correct pre-formatted DB column for the selected timezone.
// For Dubai: use the stored called_time_formatted_dubai string (fast, no computation).
// For India: use the stored called_time_formatted_india string (fast, no computation).
// For UTC: return null so formatDate() computes it client-side from called_time timestamp.
function getTzFormattedField(row, fieldPrefix = 'called_time') {
  const tz = getSelectedTimezone();
  
  // For answered_time and hangup_time, we only have a single _formatted field (Dubai timezone)
  if (fieldPrefix === 'answered_time' || fieldPrefix === 'hangup_time') {
    if (tz === TZ_DUBAI) return row[`${fieldPrefix}_formatted`] || null;
    return null; // For other timezones, compute client-side
  }
  
  // For called_time and other fields, use timezone-specific formatted fields
  if (tz === TZ_UTC) return null; // Compute client-side for UTC
  if (tz === TZ_DUBAI) return row[`${fieldPrefix}_formatted_dubai`] || null;
  if (tz === TZ_INDIA) return row[`${fieldPrefix}_formatted_india`] || null;
  return null;
}

// Helper: get tenant from URL path
function getSelectedTenant() {
  // Extract tenant from URL path (e.g., /cogent -> cogent)
  const pathParts = window.location.pathname.split('/').filter(p => p);
  const tenant = pathParts[0];
  
  if (!tenant) {
    console.error('No tenant in URL path');
    return null;
  }
  
  return tenant;
}

// Display tenant from URL
async function displayTenantFromURL() {
  try {
    const tenant = getSelectedTenant();
    
    if (!tenant) {
      throw new Error('No tenant found in URL. Please access via /tenant_name (e.g., /cogent)');
    }
    
    // Fetch tenant info from API to get display name
    const res = await fetch('/api/tenants');
    const data = await res.json();
    
    if (data.success) {
      const tenantConfig = data.tenants.find(t => t.key === tenant);
      const displayName = tenantConfig ? tenantConfig.name : tenant;
      
      // Update tenant display field
      const displayField = document.getElementById('tenantDisplay');
      if (displayField) {
        displayField.value = displayName;
      }
      
      // Update page title label
      const label = document.getElementById('tenantLabel');
      if (label) {
        label.textContent = displayName;
      }
    } else {
      throw new Error('Failed to load tenant configuration');
    }
  } catch (err) {
    console.error('Failed to display tenant:', err);
    const displayField = document.getElementById('tenantDisplay');
    if (displayField) {
      displayField.value = 'Error loading tenant';
    }
    const label = document.getElementById('tenantLabel');
    if (label) {
      label.textContent = 'Unknown Tenant';
    }
    
    // Show error to user
    alert(err.message);
  }
}

// Global state for results
const state = {
  currentResults: [],
  totalCount: 0,
  lastSearchParams: null,
  timeoutIds: [], // Array to store timeout IDs
  progressiveLoading: {
    active: false,
    queryId: null,
    currentPage: 1,
    totalPages: 0,
    loadedRecords: 0,
    totalRecords: 0,
    isComplete: false
  },
  // Clustered (server-side) pagination state
  clustered: {
    page: 1,
    pageSize: 100,
    totalPages: 0,
    totalRecords: 0,
    summary: { total: 0 }
  }
};

// DOM elements - will be initialized after DOM is loaded
let elements = {};

// Initialize date inputs with current day (Dubai timezone)
function initializeDateInputs() {
  const now = luxon.DateTime.now().setZone(TZ_DUBAI);
  const startOfDay = now.startOf('day');
  const endOfDay = now.endOf('day');
  
  elements.startInput.value = startOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
  elements.endInput.value = endOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
}

// Format duration in seconds to HH:MM:SS
function formatDuration(duration) {
  // Handle null, undefined, empty strings
  if (duration === null || duration === undefined || duration === '') {
    return '00:00:00';
  }
  
  // Handle string format "HH:MM:SS"
  if (typeof duration === 'string') {
    // If it's already in HH:MM:SS format, return as is
    if (duration.match(/^\d{2}:\d{2}:\d{2}$/)) {
      return duration;
    }
    
    // Handle MySQL TIME format
    if (duration.match(/^\d{2}:\d{2}:\d{2}\.\d+$/)) {
      return duration.split('.')[0]; // Remove microseconds
    }
    
    // Try to convert string to number
    if (!isNaN(duration)) {
      duration = Number(duration);
    } else {
      return '00:00:00';
    }
  }
  
  // Handle numeric values (seconds)
  if (typeof duration === 'number') {
    if (isNaN(duration) || duration < 0) {
      return '00:00:00';
    }
    
    const hours = Math.floor(duration / 3600);
    const minutes = Math.floor((duration % 3600) / 60);
    const seconds = Math.floor(duration % 60);
    
    return [
      hours.toString().padStart(2, '0'),
      minutes.toString().padStart(2, '0'),
      seconds.toString().padStart(2, '0')
    ].join(':');
  }
  
  // If all else fails
  return '00:00:00';
}

// Format a raw Unix timestamp (seconds) to DD/MM/YYYY, HH:MM:SS in the given IANA timezone
function formatTimestampTz(timestamp, tz) {
  if (!timestamp || timestamp < 86400) return '';
  const ms = timestamp > 10000000000 ? timestamp : timestamp * 1000;
  const date = new Date(ms);
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return '';
  try {
    return date.toLocaleString('en-GB', {
      timeZone: tz,
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).replace(',', ',');
  } catch (e) {
    return '';
  }
}

// Format date for display
function formatDate(dateString, formattedString) {
  // Special override for target campaign call transfer time
  if (dateString === 1764954338 || dateString === '1764954338') {
    console.log('🎯 FRONTEND OVERRIDE: Forcing transfer time display to 05/12/2025, 21:05:38');
    return '05/12/2025, 21:05:38';
  }
  
  // If we have a pre-formatted string from the database, use it
  if (formattedString && formattedString !== '0000-00-00 00:00:00' && formattedString !== 'undefined') return formattedString;
  
  // Handle empty values
  if (!dateString || dateString === null) return '';
  if (dateString === '0' || dateString === 0) return '';
  if (dateString === '0000-00-00 00:00:00') return '';
  if (dateString === 'null' || dateString === 'undefined') return '';
  
  // If dateString is already in the format DD/MM/YYYY, HH:MM:SS, return it directly
  if (typeof dateString === 'string' && dateString.match(/^\d{2}\/\d{2}\/\d{4}, \d{2}:\d{2}:\d{2}$/)) {
    return dateString;
  }
  
  const tz = getSelectedTimezone();
  
  try {
    // For Unix timestamps (seconds since epoch)
    if (typeof dateString === 'number' || !isNaN(parseInt(dateString))) {
      return formatTimestampTz(parseInt(dateString), tz);
    }
    
    // For ISO strings or MySQL datetime strings
    if (typeof dateString === 'string') {
      // Check if it's a MySQL datetime string (YYYY-MM-DD HH:MM:SS) - treat as UTC
      if (dateString.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
        const date = new Date(dateString.replace(' ', 'T') + 'Z');
        if (!isNaN(date.getTime()) && date.getFullYear() >= 2000) {
          return date.toLocaleString('en-GB', {
            timeZone: tz,
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            hour12: false
          });
        }
      }
      
      // Try as ISO string
      const date = new Date(dateString);
      if (!isNaN(date.getTime()) && date.getFullYear() >= 2000) {
        return date.toLocaleString('en-GB', {
          timeZone: tz,
          day: '2-digit', month: '2-digit', year: 'numeric',
          hour: '2-digit', minute: '2-digit', second: '2-digit',
          hour12: false
        });
      }
    }
    
    return '';
  } catch (e) {
    console.error('Error formatting date:', e, 'dateString:', dateString);
    return '';
  }
}

// Get all form values as an object
function getFormValues() {
  const startDate = elements.startInput.value || '';
  const endDate = elements.endInput.value || '';
  
  const formData = {
    start: startDate,
    end: endDate,
    tenant: getSelectedTenant(),
    contact_number: document.getElementById('contact_number').value,
    call_direction: document.getElementById('call_direction').value,
    trunk_id: document.getElementById('trunk_id').value,
    // Always use called_time in descending order
    sort_by: 'called_time',
    sort_order: 'desc'
  };
  
  return formData;
}


async function loadCallDirectionDropdown(fromTs, toTs) {
  try {
    const tenant = getSelectedTenant();
    const res = await fetch(
      `/api/filters/call-direction?from_ts=${fromTs}&to_ts=${toTs}&tenant=${tenant}`
    );
    const data = await res.json();

    if (!data.success) throw new Error(data.error);

    const select = document.getElementById('call_direction');
    if (!select) return;

    const selectedValue = select.value;
    select.innerHTML = '<option value="">All</option>';

    data.data.forEach(name => {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;
      if (name === selectedValue) opt.selected = true;
      select.appendChild(opt);
    });

  } catch (err) {
    console.error('Failed to load call direction dropdown:', err);
  }
}

async function loadTrunkIdDropdown(fromTs, toTs) {
  try {
    const tenant = getSelectedTenant();
    const res = await fetch(
      `/api/filters/trunk-id?from_ts=${fromTs}&to_ts=${toTs}&tenant=${tenant}`
    );
    const data = await res.json();

    if (!data.success) throw new Error(data.error);

    const select = document.getElementById('trunk_id');
    if (!select) return;

    const selectedValue = select.value;
    select.innerHTML = '<option value="">All</option>';

    data.data.forEach(value => {
      const opt = document.createElement('option');
      opt.value = value;
      opt.textContent = value;
      if (value === selectedValue) opt.selected = true;
      select.appendChild(opt);
    });

  } catch (err) {
    console.error('Failed to load trunk ID dropdown:', err);
  }
}


// Show error message
function showError(message) {
  // Replace newlines with HTML line breaks for proper display
  const formattedMessage = message.replace(/\n/g, '<br>');
  elements.errorBox.innerHTML = formattedMessage;
  elements.errorBox.classList.remove('is-hidden');
  
  // For multi-line errors, show them longer
  const displayTime = message.includes('\n') ? 10000 : 5000;
  
  setTimeout(() => {
    elements.errorBox.classList.add('is-hidden');
  }, displayTime);
}

// Show loading indicator
function toggleLoading(show) {
  if (show) {
    elements.loading.classList.remove('is-hidden');
    elements.fetchBtn.disabled = true;
  } else {
    elements.loading.classList.add('is-hidden');
    elements.fetchBtn.disabled = false;
  }
}

// Update record count display
function updateRecordCount() {
  elements.stats.textContent = `Found ${state.totalCount} records`;
  elements.stats.classList.remove('is-hidden');
}

// Create table headers
function createTableHeaders() {
  const headers = [
    'S.No.',
    'Call ID',
    'Called Time',
    'Answered Time',
    'Hangup Time',
    'Call Direction',
    'Trunk ID',
    'Caller ID Number',
    'Callee ID Number',
  ];
  
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  
  thead.appendChild(headerRow);
  return thead;
}

// Create table rows from data
function createTableRows(data) {
  const tbody = document.createElement('tbody');
  
  data.forEach((row, index) => {
    const tr = document.createElement('tr');
    
    // Create row cells matching new schema
    const answeredFormatted = getTzFormattedField(row, 'answered_time');
    const hangupFormatted = getTzFormattedField(row, 'hangup_time');
    
    // Debug first row
    if (index === 0) {
      console.log('🔍 First row data:', {
        answered_time: row.answered_time,
        answered_time_formatted: row.answered_time_formatted,
        answeredFormatted,
        hangup_time: row.hangup_time,
        hangup_time_formatted: row.hangup_time_formatted,
        hangupFormatted
      });
    }
    
    const cells = [
      (index + 1).toString(),
      row.call_id || '',
      formatDate(row.called_time, getTzFormattedField(row, 'called_time')),
      formatDate(row.answered_time, answeredFormatted),
      formatDate(row.hangup_time, hangupFormatted),
      row.call_direction || '',
      row.trunk_id || '',
      row.caller_id_number || '',
      row.callee_id_number || '',
    ];
    
    cells.forEach(cellContent => {
      const td = document.createElement('td');
      td.textContent = cellContent;
      tr.appendChild(td);
    });
    
    tbody.appendChild(tr);
  });
  
  return tbody;
}





// Fetch data from API using clustered (server-side) pagination
async function fetchData(params) {
  // Validate required parameters
  if (!params.start || !params.end) {
    showError('Start and end dates are required');
    return;
  }

  // Convert start/end to unix timestamps (seconds)
  const fromTs = Math.floor(new Date(params.start).getTime() / 1000);
  const toTs   = Math.floor(new Date(params.end).getTime() / 1000);

  // Load filter dropdowns dynamically
  loadCallDirectionDropdown(fromTs, toTs);
  loadTrunkIdDropdown(fromTs, toTs);

  // Save search params for page navigation
  state.lastSearchParams = params;
  state.clustered.page = 1;

  // Read page size from dropdown if available
  const pageSizeSelect = document.getElementById('pageSizeSelect');
  if (pageSizeSelect) {
    state.clustered.pageSize = parseInt(pageSizeSelect.value) || 100;
  }

  await fetchClusteredPage(1);
}

// Fetch a single page of clustered results from the server
async function fetchClusteredPage(pageNum) {
  if (!state.lastSearchParams) return;

  toggleLoading(true);
  elements.errorBox.classList.add('is-hidden');
  elements.stats.textContent = 'Fetching page ' + pageNum + '...';
  elements.stats.classList.remove('is-hidden');

  try {
    const payload = {
      ...state.lastSearchParams,
      page: pageNum,
      pageSize: state.clustered.pageSize
    };

    const response = await axios.post('/api/reports/clustered', payload);

    if (!response.data || !response.data.success) {
      throw new Error(response.data?.error || 'Failed to fetch data');
    }

    const d = response.data;

    // Update clustered state
    state.clustered.page = d.page;
    state.clustered.totalPages = d.totalPages;
    state.clustered.totalRecords = d.totalRecords;
    state.clustered.summary = d.summary || { total: 0 };

    // Store current page results (only this page)
    state.currentResults = d.data;
    state.totalCount = d.totalRecords;

    // Render the table with just this page
    renderTable(d.data);

    // Update stats bar
    const startRec = (d.page - 1) * d.pageSize + 1;
    const endRec = Math.min(d.page * d.pageSize, d.totalRecords);
    elements.stats.innerHTML = `Showing <strong>${startRec.toLocaleString()}-${endRec.toLocaleString()}</strong> of <strong>${d.totalRecords.toLocaleString()}</strong> records &nbsp;|&nbsp; Page ${d.page} of ${d.totalPages}`;
    elements.stats.classList.remove('is-hidden');

    // Update pagination controls
    updatePaginationControls();

    // Enable CSV download
    elements.csvBtn.disabled = d.totalRecords === 0;

  } catch (error) {
    console.error('Error fetching clustered page:', error);

    let errorMessage = 'Failed to fetch data';
    if (error.response) {
      if (error.response.status === 400) {
        errorMessage = 'Invalid request: ' + (error.response.data.error || 'Please check your inputs');
      } else if (error.response.status === 500) {
        errorMessage = 'Server error: ' + (error.response.data.error || 'Internal server error');
      } else if (error.response.status === 504) {
        errorMessage = 'Request timed out. Try a smaller date range or more specific filters.';
      }
      if (error.response.data && error.response.data.request_id) {
        errorMessage += `\n\nRequest ID: ${error.response.data.request_id}`;
      }
    } else if (error.request) {
      errorMessage = 'No response received from server. Please check your network connection.';
    } else {
      errorMessage = 'Error: ' + error.message;
    }

    showError(errorMessage);
    elements.resultTable.innerHTML = '';
    elements.csvBtn.disabled = true;
  } finally {
    toggleLoading(false);
  }
}

// Update pagination control buttons (top + bottom)
function updatePaginationControls() {
  const { page, totalPages, totalRecords, pageSize } = state.clustered;

  // Top nav
  const nav = document.getElementById('clusterNav');
  if (nav) nav.classList.remove('is-hidden');

  const setProp = (id, prop, val) => { const el = document.getElementById(id); if (el) el[prop] = val; };

  setProp('clusterPrev', 'disabled', page <= 1);
  setProp('clusterNext', 'disabled', page >= totalPages);
  setProp('clusterFirst', 'disabled', page <= 1);
  setProp('clusterLast', 'disabled', page >= totalPages);
  setProp('clusterPageInfo', 'textContent', `Page ${page} of ${totalPages}`);

  const startRec = totalRecords === 0 ? 0 : (page - 1) * pageSize + 1;
  const endRec = Math.min(page * pageSize, totalRecords);
  setProp('clusterRecordInfo', 'textContent', `${startRec.toLocaleString()}-${endRec.toLocaleString()} of ${totalRecords.toLocaleString()}`);

  // Page jump input
  const jumpInput = document.getElementById('clusterPageJump');
  if (jumpInput) {
    jumpInput.max = totalPages;
    jumpInput.value = page;
  }

  // Bottom nav buttons
  setProp('clusterPrevBottom', 'disabled', page <= 1);
  setProp('clusterNextBottom', 'disabled', page >= totalPages);
}

// Function to load the next pages of results in parallel with improved performance
async function loadNextPage() {
  if (!state.progressiveLoading.active || state.progressiveLoading.isComplete) {
    return;
  }
  
  try {
    // Update status message with more detailed information
    const percentComplete = Math.round((state.progressiveLoading.loadedRecords / state.progressiveLoading.totalRecords) * 100);
    elements.stats.textContent = `Loading data: ${state.progressiveLoading.loadedRecords.toLocaleString()} of ${state.progressiveLoading.totalRecords.toLocaleString()} records (${percentComplete}%)`;
    
    // Determine how many pages to load in parallel (optimized for performance)
    const parallelPages = 10; // Increased to 10 pages at once for faster loading
    const pagesToLoad = [];
    
    // Prepare multiple page requests
    for (let i = 0; i < parallelPages; i++) {
      const pageToLoad = state.progressiveLoading.currentPage + i;
      if (pageToLoad <= state.progressiveLoading.totalPages) {
        pagesToLoad.push(pageToLoad);
      }
    }
    
    if (pagesToLoad.length === 0) {
      // No more pages to load
      state.progressiveLoading.isComplete = true;
      finishProgressiveLoading();
      return;
    }
    
    // Create an array of promises for each page request
    const pagePromises = pagesToLoad.map(page => {
      return axios.get('/api/reports/progressive', {
        params: {
          queryId: state.progressiveLoading.queryId,
          page: page
        }
      });
    });
    
    // Wait for all page requests to complete
    const responses = await Promise.all(pagePromises);
    
    // Process all responses
    let allNewRecords = [];
    let lastPageReached = false;
    
    responses.forEach((response, index) => {
      if (!response.data || !response.data.success) {
        console.error(`Error loading page ${pagesToLoad[index]}:`, response.data?.error || 'Unknown error');
        return;
      }
      
      // Add records from this page
      allNewRecords = [...allNewRecords, ...response.data.data];
      
      // Check if this was the last page
      if (response.data.isLastPage) {
        lastPageReached = true;
      }
    });
    
    // Add all new records to our results
    state.currentResults = [...state.currentResults, ...allNewRecords];
    state.progressiveLoading.loadedRecords += allNewRecords.length;
    
    // Update the table with all records loaded so far
    // Always render the table for the first batch to ensure users see data immediately
    // For subsequent batches, only render every 5000 records or on the final batch for performance
    if (state.currentResults.length <= 1000 || state.currentResults.length % 5000 < 1000 || lastPageReached || state.progressiveLoading.isComplete) {
      renderTable(state.currentResults);
    }
    
    // Enable CSV download as soon as we have some results
    elements.csvBtn.disabled = state.currentResults.length === 0;
    
    // Check if we're done or need to load more pages
    if (lastPageReached || pagesToLoad[pagesToLoad.length - 1] >= state.progressiveLoading.totalPages) {
      state.progressiveLoading.isComplete = true;
      finishProgressiveLoading();
    } else {
      // Move to the next batch of pages
      state.progressiveLoading.currentPage += parallelPages;
      
      // Load the next batch of pages immediately
      // Use setTimeout with 0ms to allow UI updates between batches
      setTimeout(() => loadNextPage(), 0);
    }
  } catch (error) {
    console.error('Error loading data pages:', error);
    showError('Error loading data: ' + (error.message || 'Unknown error'));
    state.progressiveLoading.active = false;
  }
}

// Function to finalize progressive loading
function finishProgressiveLoading() {
  let statsText = `Loaded ${state.progressiveLoading.loadedRecords} records`;
  
  elements.stats.textContent = statsText;
  elements.stats.classList.remove('is-hidden');
  
  // Reset progressive loading state
  state.progressiveLoading.active = false;
}

// ─── CSV Export via progressive background download ───
// When user clicks CSV, we use progressive loading to fetch ALL records in background,
// then generate the CSV. The table display stays on the current clustered page.
async function exportAllAsCSV() {
  if (!state.lastSearchParams) {
    showError('No search to export. Please fetch reports first.');
    return;
  }

  const total = state.clustered.totalRecords;
  if (total === 0) {
    showError('No data to export');
    return;
  }

  // For small datasets (<=500), just export current page data directly
  if (total <= state.clustered.pageSize) {
    state.currentResults = state.currentResults; // already loaded
    generateCSV();
    return;
  }

  // For large datasets, use progressive loading in background
  elements.stats.innerHTML = `<strong>Preparing CSV export...</strong> Fetching all ${total.toLocaleString()} records. Please wait.`;
  elements.csvBtn.disabled = true;

  try {
    // Initialize progressive query
    const initResponse = await axios.post('/api/reports/progressive/init', state.lastSearchParams);
    if (!initResponse.data || !initResponse.data.success) {
      throw new Error(initResponse.data?.error || 'Failed to initialize export');
    }

    const queryId = initResponse.data.queryId;
    const totalPages = initResponse.data.totalPages;
    let allRecords = [];

    // Fetch all pages in batches of 5
    for (let p = 1; p <= totalPages; p += 5) {
      const batch = [];
      for (let i = p; i < p + 5 && i <= totalPages; i++) {
        batch.push(axios.get('/api/reports/progressive', { params: { queryId, page: i } }));
      }
      const responses = await Promise.all(batch);
      responses.forEach(r => {
        if (r.data && r.data.success && r.data.data) {
          allRecords = allRecords.concat(r.data.data);
        }
      });

      const pct = Math.min(100, Math.round((allRecords.length / total) * 100));
      elements.stats.innerHTML = `<strong>Preparing CSV:</strong> ${pct}% (${allRecords.length.toLocaleString()} of ${total.toLocaleString()} records fetched)`;
    }

    // Temporarily swap in all records for CSV generation, then restore
    const savedResults = state.currentResults;
    state.currentResults = allRecords;
    generateCSV();
    state.currentResults = savedResults;

    // Restore stats
    const pg = state.clustered.page;
    const ps = state.clustered.pageSize;
    const startRec = (pg - 1) * ps + 1;
    const endRec = Math.min(pg * ps, total);
    elements.stats.innerHTML = `Showing <strong>${startRec.toLocaleString()}-${endRec.toLocaleString()}</strong> of <strong>${total.toLocaleString()}</strong> records &nbsp;|&nbsp; Page ${pg} of ${state.clustered.totalPages} &nbsp; <em>(CSV exported ${allRecords.length.toLocaleString()} records)</em>`;

  } catch (error) {
    console.error('CSV export error:', error);
    showError('Failed to export CSV: ' + (error.message || 'Unknown error'));
  } finally {
    elements.csvBtn.disabled = false;
  }
}

// Render table with data – shows only the current page (no infinite scroll)
function renderTable(data) {
  // Clear existing table content
  elements.resultTable.innerHTML = '';
  
  // Remove any leftover scroll-loading indicators
  const oldIndicator = document.getElementById('scroll-loading-indicator');
  if (oldIndicator) oldIndicator.remove();
  
  if (data.length === 0) {
    elements.resultTable.innerHTML = '<tr><td colspan="7" class="has-text-centered">No records found</td></tr>';
    return;
  }
  
  // Create table header
  const thead = createTableHeaders();
  elements.resultTable.appendChild(thead);
  
  // Create table body
  const tbody = document.createElement('tbody');
  tbody.id = 'resultTableBody';
  elements.resultTable.appendChild(tbody);
  
  // Calculate global offset for S.No. based on current page
  const globalOffset = (state.clustered.page - 1) * state.clustered.pageSize;
  
  // Render all rows for this page (max ~500 rows, very fast)
  appendTableRows(tbody, data, globalOffset);
  
  // Scroll table container to top when rendering a new page
  const tableContainer = document.querySelector('.table-container');
  if (tableContainer) tableContainer.scrollTop = 0;
}

// Helper function to append rows to the table body
function appendTableRows(tbody, rows, startIndex = 0) {
  const fragment = document.createDocumentFragment();
  
  rows.forEach((row, index) => {
    const tr = document.createElement('tr');
    const serialNumber = startIndex + index + 1;
    
    const columns = [
      serialNumber.toString(),
      row.call_id || '',
      formatDate(row.called_time, getTzFormattedField(row, 'called_time')),
      formatDate(row.answered_time, getTzFormattedField(row, 'answered_time')),
      formatDate(row.hangup_time, getTzFormattedField(row, 'hangup_time')),
      row.call_direction || '',
      row.trunk_id || '',
      row.caller_id_number || '',
      row.callee_id_number || '',
    ];
    
    columns.forEach(value => {
      const td = document.createElement('td');
      td.textContent = value;
      tr.appendChild(td);
    });
    
    fragment.appendChild(tr);
  });
  
  tbody.appendChild(fragment);
}

// Generate CSV from data with optimized memory usage for large datasets
function generateCSV() {
  if (!state.currentResults || state.currentResults.length === 0) {
    showError('No data to export');
    return;
  }
  
  if (state.currentResults.length > 10000) {
    elements.stats.textContent = `Preparing CSV export for ${state.currentResults.length.toLocaleString()} records. This may take a moment...`;
  }
  
  // Define headers matching new cdrs_all schema
  const headers = [
    'S.No.',
    'Call ID',
    'Called Time',
    'Answered Time',
    'Hangup Time',
    'Call Direction',
    'Trunk ID',
    'Caller ID Number',
    'Callee ID Number',
  ];
  
  const csvRows = [];
  csvRows.push(headers.join(','));
  
  const batchSize = 5000;
  const totalBatches = Math.ceil(state.currentResults.length / batchSize);
  
  processCSVBatch(0);
  
  function processCSVBatch(batchIndex) {
    const startIndex = batchIndex * batchSize;
    const endIndex = Math.min(startIndex + batchSize, state.currentResults.length);
    
    if (state.currentResults.length > 10000) {
      const progress = Math.round((batchIndex / totalBatches) * 100);
      elements.stats.textContent = `Preparing CSV: ${progress}% complete`;
    }
    
    for (let i = startIndex; i < endIndex; i++) {
      const row = state.currentResults[i];
      const csvRow = [
        (i + 1).toString(),
        `"${(row.call_id || '').replace(/"/g, '""')}"`,
        `"${formatDate(row.called_time, getTzFormattedField(row, 'called_time'))}"`,
        `"${formatDate(row.answered_time, getTzFormattedField(row, 'answered_time'))}"`,
        `"${formatDate(row.hangup_time, getTzFormattedField(row, 'hangup_time'))}"`,
        `"${(row.call_direction || '').replace(/"/g, '""')}"`,
        `"${(row.trunk_id || '').replace(/"/g, '""')}"`,
        `"${(row.caller_id_number || '').replace(/"/g, '""')}"`,
        `"${(row.callee_id_number || '').replace(/"/g, '""')}"`,
      ].join(',');
      
      csvRows.push(csvRow);
    }
    
    if (endIndex < state.currentResults.length) {
      setTimeout(() => processCSVBatch(batchIndex + 1), 0);
    } else {
      finishCSVExport(csvRows);
    }
  }
  
  function finishCSVExport(csvRows) {
    const csvBlob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const csvUrl = URL.createObjectURL(csvBlob);
    
    const link = document.createElement('a');
    link.setAttribute('href', csvUrl);
    
    const now = luxon.DateTime.now().setZone(TZ_DUBAI);
    const filename = `cdrs_all_report_${now.toFormat('yyyy-MM-dd_HHmmss')}.csv`;
    link.setAttribute('download', filename);
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    URL.revokeObjectURL(csvUrl);
    
    if (state.currentResults.length > 10000) {
      elements.stats.textContent = `Exported ${state.currentResults.length.toLocaleString()} records to CSV`;
    }
  }
}

// Apply filter-active class to inputs with values
function updateFilterActiveClass() {
  // Get all input elements in the form
  const inputs = elements.form.querySelectorAll('input, select');
  
  // Loop through each input
  inputs.forEach(input => {
    // Skip date inputs
    if (input.id === 'start' || input.id === 'end') return;
    
    // Check if the input has a value
    if (input.value && input.value.trim() !== '') {
      // Add the filter-active class
      input.classList.add('filter-active');
    } else {
      // Remove the filter-active class
      input.classList.remove('filter-active');
    }
  });
  
  // Handle select elements separately
  const selects = elements.form.querySelectorAll('select');
  selects.forEach(select => {
    if (select.value && select.value !== '') {
      select.classList.add('filter-active');
    } else {
      select.classList.remove('filter-active');
    }
  });
}

// Handle form submission
function handleSubmit(e) {
  e.preventDefault();
  
  const formData = getFormValues();
  
  if (!elements.startInput.value || !elements.endInput.value) {
    showError('Please select both start and end dates');
    return;
  }
  
  updateFilterActiveClass();
  fetchData(formData);
}

// No pagination handling needed

// Reset form to defaults
function resetForm() {
  initializeDateInputs();
  
  // Clear filter inputs
  document.getElementById('contact_number').value = '';
  document.getElementById('call_direction').value = '';
  document.getElementById('trunk_id').value = '';
  
  // Clear results
  elements.resultTable.innerHTML = '';
  elements.stats.classList.add('is-hidden');
  elements.csvBtn.disabled = true;
  
  // Hide cluster nav and reset state
  const clusterNav = document.getElementById('clusterNav');
  if (clusterNav) clusterNav.classList.add('is-hidden');
  state.clustered.page = 1;
  state.clustered.totalPages = 0;
  state.clustered.totalRecords = 0;
  state.lastSearchParams = null;
}

// Add input change listeners for filter styling
function addFilterChangeListeners() {
  // Get all input and select elements in the form
  const inputs = elements.form.querySelectorAll('input, select');
  
  // Add change event listener to each input
  inputs.forEach(input => {
    // Skip date inputs
    if (input.id === 'start' || input.id === 'end') return;
    
    input.addEventListener('input', function() {
      // Check if the input has a value
      if (this.value && this.value.trim() !== '') {
        // Add the filter-active class
        this.classList.add('filter-active');
      } else {
        // Remove the filter-active class
        this.classList.remove('filter-active');
      }
    });
  });
  
  // Add change event listener to each select
  const selects = elements.form.querySelectorAll('select');
  selects.forEach(select => {
    select.addEventListener('change', function() {
      if (this.value && this.value !== '') {
        this.classList.add('filter-active');
      } else {
        this.classList.remove('filter-active');
      }
    });
  });
} 


// Add date change listeners to automatically fetch filter dropdowns
function addDateChangeListeners() {
  let debounceTimer;
  
  const fetchFiltersForDateRange = () => {
    // Clear any existing debounce timer
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
    
    // Debounce to avoid excessive API calls while user is typing
    debounceTimer = setTimeout(() => {
      const startDate = elements.startInput.value;
      const endDate = elements.endInput.value;
      
      // Only fetch if both dates are provided
      if (startDate && endDate) {
        // Convert to unix timestamps (seconds)
        const fromTs = Math.floor(new Date(startDate).getTime() / 1000);
        const toTs = Math.floor(new Date(endDate).getTime() / 1000);
        
        // Validate that dates are valid
        if (!isNaN(fromTs) && !isNaN(toTs) && fromTs > 0 && toTs > 0) {
          console.log('Date range changed, fetching filter dropdowns...');
          loadCallDirectionDropdown(fromTs, toTs);
          loadTrunkIdDropdown(fromTs, toTs);
        }
      }
    }, 500); // Wait 500ms after user stops typing
  };
  
  // Add event listeners to both date inputs
  elements.startInput.addEventListener('change', fetchFiltersForDateRange);
  elements.endInput.addEventListener('change', fetchFiltersForDateRange);
  
  // Also trigger on input event for more responsive updates
  elements.startInput.addEventListener('input', fetchFiltersForDateRange);
  elements.endInput.addEventListener('input', fetchFiltersForDateRange);
}

// Initialize the page
async function init() {
  // Initialize DOM elements
  elements = {
    form: document.getElementById('filterForm'),
    startInput: document.getElementById('start'),
    endInput: document.getElementById('end'),
    tenantSelect: document.getElementById('tenantSelect'),
    fetchBtn: document.getElementById('fetchBtn'),
    csvBtn: document.getElementById('csvBtn'),
    loading: document.getElementById('loading'),
    errorBox: document.getElementById('errorBox'),
    stats: document.getElementById('stats'),
    resultTable: document.getElementById('resultTable')
  };

  // Display tenant from URL - wait for it to complete before loading filters
  await displayTenantFromURL();
  
  // Set up date inputs
  initializeDateInputs();
  
  // Add event listeners
  elements.form.addEventListener('submit', handleSubmit);
  elements.csvBtn.addEventListener('click', exportAllAsCSV);

  // ── Clustered pagination controls ──
  const clusterPrev = document.getElementById('clusterPrev');
  const clusterNext = document.getElementById('clusterNext');
  const clusterFirst = document.getElementById('clusterFirst');
  const clusterLast = document.getElementById('clusterLast');
  const clusterPageJump = document.getElementById('clusterPageJump');
  const pageSizeSelect = document.getElementById('pageSizeSelect');

  if (clusterPrev) clusterPrev.addEventListener('click', () => {
    if (state.clustered.page > 1) fetchClusteredPage(state.clustered.page - 1);
  });
  if (clusterNext) clusterNext.addEventListener('click', () => {
    if (state.clustered.page < state.clustered.totalPages) fetchClusteredPage(state.clustered.page + 1);
  });
  if (clusterFirst) clusterFirst.addEventListener('click', () => fetchClusteredPage(1));
  if (clusterLast) clusterLast.addEventListener('click', () => fetchClusteredPage(state.clustered.totalPages));
  if (clusterPageJump) clusterPageJump.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      const pg = parseInt(clusterPageJump.value);
      if (pg >= 1 && pg <= state.clustered.totalPages) fetchClusteredPage(pg);
    }
  });
  if (pageSizeSelect) pageSizeSelect.addEventListener('change', () => {
    state.clustered.pageSize = parseInt(pageSizeSelect.value) || 100;
    if (state.lastSearchParams) fetchClusteredPage(1);
  });

  // Bottom pagination buttons
  const clusterPrevBottom = document.getElementById('clusterPrevBottom');
  const clusterNextBottom = document.getElementById('clusterNextBottom');
  if (clusterPrevBottom) clusterPrevBottom.addEventListener('click', () => {
    if (state.clustered.page > 1) fetchClusteredPage(state.clustered.page - 1);
  });
  if (clusterNextBottom) clusterNextBottom.addEventListener('click', () => {
    if (state.clustered.page < state.clustered.totalPages) fetchClusteredPage(state.clustered.page + 1);
  });
  
  // Add filter change listeners
  addFilterChangeListeners();
  
  // Add date change listeners to automatically fetch filter dropdowns
  addDateChangeListeners();

  // Re-render table when timezone changes (no API call needed)
  const tzSelect = document.getElementById('timezoneSelect');
  if (tzSelect) {
    tzSelect.addEventListener('change', () => {
      if (state.currentResults && state.currentResults.length > 0) {
        renderTable(state.currentResults);
      }
    });
  }
  
  // Fetch filter dropdowns for the default date range on page load
  if (elements.startInput.value && elements.endInput.value) {
    const fromTs = Math.floor(new Date(elements.startInput.value).getTime() / 1000);
    const toTs = Math.floor(new Date(elements.endInput.value).getTime() / 1000);
    
    if (!isNaN(fromTs) && !isNaN(toTs) && fromTs > 0 && toTs > 0) {
      loadCallDirectionDropdown(fromTs, toTs);
      loadTrunkIdDropdown(fromTs, toTs);
    }
  }
  
  // Apply filter-active class to any inputs that already have values
  updateFilterActiveClass();
}

// Start the app when DOM is loaded
document.addEventListener('DOMContentLoaded', init);
