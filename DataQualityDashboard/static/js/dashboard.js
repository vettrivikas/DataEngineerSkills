// Dashboard JavaScript functionality

let currentSchemas = [];
let currentTables = [];

function initializeDashboard() {
    console.log('Initializing dashboard...');
    
    // Set default date to today
    const runDateInput = document.getElementById('runDate');
    if (runDateInput) {
        const today = new Date().toISOString().split('T')[0];
        runDateInput.value = today;
    }
    
    // Setup event listeners
    const schemaSelect = document.getElementById('schemaSelect');
    const tableSelect = document.getElementById('tableSelect');
    const tableSelectForm = document.getElementById('tableSelectForm');
    
    if (schemaSelect) {
        schemaSelect.addEventListener('change', handleSchemaChange);
    }
    
    if (tableSelectForm) {
        tableSelectForm.addEventListener('submit', handleFormSubmit);
    }
}

async function handleSchemaChange(event) {
    const schemaName = event.target.value;
    const tableSelect = document.getElementById('tableSelect');
    
    // Reset table selection
    tableSelect.innerHTML = '<option value="">Select Table...</option>';
    tableSelect.disabled = true;
    
    if (!schemaName) return;
    
    try {
        // Show loading state
        tableSelect.innerHTML = '<option value="">Loading tables...</option>';
        
        // Fetch tables for selected schema
        const response = await fetch(`/api/tables/${encodeURIComponent(schemaName)}`);
        const data = await response.json();
        
        if (data.success) {
            // Populate table dropdown
            tableSelect.innerHTML = '<option value="">Select Table...</option>';
            data.tables.forEach(table => {
                const option = document.createElement('option');
                option.value = table.name;
                option.textContent = table.name;
                tableSelect.appendChild(option);
            });
            tableSelect.disabled = false;
            currentTables = data.tables;
        } else {
            throw new Error(data.error || 'Failed to fetch tables');
        }
    } catch (error) {
        console.error('Error fetching tables:', error);
        tableSelect.innerHTML = '<option value="">Error loading tables</option>';
        showAlert('Error fetching tables: ' + error.message, 'danger');
    }
}

async function handleFormSubmit(event) {
    event.preventDefault();
    
    const schemaSelect = document.getElementById('schemaSelect');
    const tableSelect = document.getElementById('tableSelect');
    const runDateInput = document.getElementById('runDate');
    
    const schemaName = schemaSelect.value;
    const tableName = tableSelect.value;
    const runDate = runDateInput.value;
    
    if (!schemaName || !tableName) {
        showAlert('Please select both schema and table', 'warning');
        return;
    }
    
    // Build URL with parameters
    let url = `/quality-report/${encodeURIComponent(schemaName)}/${encodeURIComponent(tableName)}`;
    if (runDate) {
        url += `?run_date=${runDate}`;
    }
    
    // Show loading spinner
    const loadingSpinner = document.getElementById('loadingSpinner');
    if (loadingSpinner) {
        loadingSpinner.style.display = 'block';
    }
    
    // Navigate to quality report
    window.location.href = url;
}

function initializeCharts(overallScores, cdeScores) {
    console.log('Initializing charts with data:', { overallScores, cdeScores });
    
    // Overall scores chart
    const overallCtx = document.getElementById('overallScoresChart');
    if (overallCtx) {
        createBarChart(overallCtx, 'Overall Data Quality Scores', overallScores);
    }
    
    // CDE scores chart
    const cdeCtx = document.getElementById('cdeScoresChart');
    if (cdeCtx) {
        createBarChart(cdeCtx, 'Critical Data Elements Scores', cdeScores);
    }
}

function createBarChart(ctx, title, data) {
    // Filter out null values and prepare data
    const labels = [];
    const scores = [];
    const colors = [];
    
    Object.entries(data).forEach(([dimension, score]) => {
        if (score !== null && score !== undefined) {
            labels.push(dimension.charAt(0).toUpperCase() + dimension.slice(1));
            scores.push(score);
            
            // Color coding based on score
            if (score >= 70) {
                colors.push('rgba(40, 167, 69, 0.8)'); // Green
            } else if (score >= 40) {
                colors.push('rgba(255, 193, 7, 0.8)'); // Yellow
            } else {
                colors.push('rgba(220, 53, 69, 0.8)'); // Red
            }
        }
    });
    
    if (labels.length === 0) {
        // Show message if no data
        ctx.getContext('2d').fillStyle = '#6c757d';
        ctx.getContext('2d').font = '14px Arial';
        ctx.getContext('2d').fillText('No data available', 50, 100);
        return;
    }
    
    new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Score (%)',
                data: scores,
                backgroundColor: colors,
                borderColor: colors.map(color => color.replace('0.8', '1')),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: title,
                    color: '#ffffff'
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        color: '#ffffff',
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                x: {
                    ticks: {
                        color: '#ffffff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            }
        }
    });
}

function showAlert(message, type = 'info') {
    // Create alert element
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // Insert at top of main container
    const container = document.querySelector('.container');
    const firstChild = container.firstElementChild;
    container.insertBefore(alertDiv, firstChild);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 5000);
}

// Utility functions
function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatPercentage(num, decimals = 1) {
    return (num || 0).toFixed(decimals) + '%';
}

// Export functions for global access
window.initializeDashboard = initializeDashboard;
window.initializeCharts = initializeCharts;
window.handleSchemaChange = handleSchemaChange;
window.handleFormSubmit = handleFormSubmit;
