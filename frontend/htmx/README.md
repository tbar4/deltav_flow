# DeltaV Flow Frontend (HTMX Version)

This is a mock frontend dashboard for the DeltaV Flow data pipeline scheduler using HTMX for enhanced interactivity.

## Features

- **HTMX-powered Interactivity**: Dynamic content loading without full page refreshes
- **Real-time Dashboard**: View pipeline status, metrics, and activity at a glance
- **Pipeline Monitoring**: Track active pipelines with their sources, destinations, and performance
- **Metrics Visualization**: Interactive charts showing throughput and processing times
- **Activity Log**: Recent pipeline activities and events
- **Pipeline Details Modal**: Detailed view of individual pipelines
- **Dark Mode Support**: Toggle between light and dark themes with persistent preferences
- **Responsive Design**: Works on desktop and mobile devices

## Quick Start

1. Start the server:
   ```bash
   cd frontend/htmx
   python server.py
   ```

2. Navigate to `http://localhost:8000` in your browser

## HTMX Features

This version demonstrates several HTMX capabilities:

- **Partial Page Updates**: Content sections update independently
- **Modal Dialogs**: Pipeline details displayed in overlays
- **Loading States**: Visual feedback during requests
- **Auto-refresh**: Automatic updates without manual intervention
- **Error Handling**: Graceful degradation when requests fail

## API Endpoints

The mock server provides these endpoints:

- `/api/dashboard` - Main dashboard content
- `/api/pipelines` - Pipeline cards section
- `/api/pipeline/details/{id}` - Detailed pipeline information
- `/api/activity` - Recent activity log
- `/api/refresh` - Trigger dashboard refresh
- `/api/close-modal` - Close modal dialogs

## How It Works

1. **Initial Load**: The main `index.html` loads and immediately requests dashboard content via HTMX
2. **Dashboard Content**: Loaded from `dashboard.html` which contains all static content plus HTMX attributes
3. **Dynamic Updates**: Buttons with `hx-get` attributes fetch updated content from the server
4. **Modals**: Pipeline details are loaded in modal dialogs without page navigation
5. **Auto-refresh**: The timestamp updates automatically every 30 seconds

## Technologies Used

- **HTML5/CSS3/JavaScript**: Modern web standards
- **Tailwind CSS**: Utility-first CSS framework
- **HTMX**: High power tools for HTML
- **Chart.js**: Data visualization library
- **Font Awesome**: Icon library
- **Python**: Simple HTTP server for serving files

## Browser Compatibility

This dashboard works on all modern browsers that support:
- ES6 JavaScript
- CSS Grid and Flexbox
- HTMX (automatically loads from CDN)

Supported browsers:
- Chrome/Chromium 80+
- Firefox 75+
- Safari 13+
- Edge 80+

## Future Enhancements

When connecting to the actual DeltaV Flow backend:

1. **REST API Integration**: Replace mock endpoints with real API calls
2. **WebSocket Support**: Real-time updates using HTMX websockets extension
3. **Form Handling**: Pipeline configuration forms with HTMX form submission
4. **History Navigation**: Browser back/forward button support with HTMX history
5. **Enhanced Loading States**: Progress indicators for long-running operations
6. **Server-Sent Events**: Continuous updates for pipeline status

## Development

To modify the frontend:

1. Edit `index.html` for overall page structure
2. Modify `dashboard.html` for main dashboard content
3. Update `pipeline-details.html` for modal content
4. Enhance `scripts.js` for client-side interactions
5. Extend `server.py` for new API endpoints

## Deployment

For production deployment:

1. Replace the Python mock server with a proper backend
2. Host static files on a CDN
3. Implement proper authentication and authorization
4. Add HTTPS support
5. Optimize asset loading and caching