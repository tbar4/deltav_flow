// HTMX configuration and utility functions

document.addEventListener('DOMContentLoaded', function() {
    // Configure HTMX to show loading states
    document.body.addEventListener('htmx:beforeRequest', function(evt) {
        // Add loading class to target element
        const target = evt.detail.target;
        target.classList.add('loading');
    });
    
    document.body.addEventListener('htmx:afterOnLoad', function(evt) {
        // Remove loading class from target element
        const target = evt.detail.target;
        target.classList.remove('loading');
        
        // Reinitialize any charts if they were updated
        if (target.querySelector('#throughputChart') || target.querySelector('#processingTimeChart')) {
            initializeCharts();
        }
    });
    
    // Global error handling for HTMX requests
    document.body.addEventListener('htmx:responseError', function(evt) {
        console.error('HTMX request failed:', evt);
        alert('Failed to load data. Please try again.');
    });
});

// Function to initialize charts
function initializeCharts() {
    // Destroy existing charts if they exist
    Chart.getChart('throughputChart')?.destroy();
    Chart.getChart('processingTimeChart')?.destroy();
    
    // Mock data for charts
    const throughputData = {
        labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', '24:00'],
        datasets: [{
            label: 'Records Processed',
            data: [120, 150, 180, 220, 280, 240, 190],
            borderColor: 'rgb(59, 130, 246)',
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            tension: 0.4
        }]
    };

    const processingTimeData = {
        labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', '24:00'],
        datasets: [{
            label: 'Processing Time (ms)',
            data: [200, 180, 220, 260, 234, 210, 195],
            borderColor: 'rgb(16, 185, 129)',
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            tension: 0.4
        }]
    };

    // Initialize charts
    const throughputCtx = document.getElementById('throughputChart');
    if (throughputCtx) {
        new Chart(throughputCtx, {
            type: 'line',
            data: throughputData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    const processingTimeCtx = document.getElementById('processingTimeChart');
    if (processingTimeCtx) {
        new Chart(processingTimeCtx, {
            type: 'line',
            data: processingTimeData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

// Function to close modals
function closeModal() {
    const modal = document.getElementById('pipeline-modal');
    if (modal) {
        modal.remove();
    }
}

// Auto-refresh functionality
setInterval(() => {
    const lastUpdated = document.getElementById('lastUpdated');
    if (lastUpdated) {
        const now = new Date();
        lastUpdated.textContent = now.toLocaleTimeString();
    }
}, 30000);