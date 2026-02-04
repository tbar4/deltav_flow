# DeltaV Flow Frontend

This is a mock frontend dashboard for the DeltaV Flow data pipeline scheduler.

## Features

- **Real-time Dashboard**: View pipeline status, metrics, and activity at a glance
- **Pipeline Monitoring**: Track active pipelines with their sources, destinations, and performance
- **Metrics Visualization**: Interactive charts showing throughput and processing times
- **Activity Log**: Recent pipeline activities and events
- **Responsive Design**: Works on desktop and mobile devices

## Quick Start

1. Open `index.html` in your web browser:
   ```bash
   # Simply open the file in your browser
   open frontend/index.html
   ```

   Or serve it with a simple HTTP server:
   ```bash
   # Using Python
   cd frontend
   python -m http.server 8080
   
   # Using Node.js
   npx serve .
   ```

2. Navigate to `http://localhost:8080` (if using a server) or view the file directly

## Dashboard Components

### Status Overview
- **Active Pipelines**: Number of currently running pipelines
- **Records Processed**: Total records processed today
- **Avg Processing Time**: Average time per record
- **Success Rate**: Pipeline success percentage

### Pipeline Cards
Each pipeline card displays:
- Pipeline name and data flow type
- Current status (Active/Warning/Error)
- Source and destination information
- Execution interval
- Last run time
- Records processed today

### Metrics Charts
- **Throughput Chart**: Records processed per hour over time
- **Processing Time Chart**: Average processing time trends

### Activity Log
Recent events including:
- Successful pipeline completions
- Warnings and errors
- System events

## Mock Data

The frontend currently uses hardcoded mock data that simulates a typical DeltaV Flow deployment:

- **3 Active Pipelines**: SpaceDev News, HTTP Default, and PDF Fetch
- **Simulated Metrics**: Realistic throughput and processing times
- **Status Updates**: Simulated pipeline activities and events

## Future Enhancements

When ready to connect to the actual DeltaV Flow backend:

1. **REST API Integration**: Replace mock data with API calls to the DeltaV Flow server
2. **Real-time Updates**: Implement WebSocket connections for live data
3. **Pipeline Management**: Add controls to start/stop/configure pipelines
4. **Authentication**: Add user authentication and authorization
5. **Configuration UI**: Interface to manage pipeline configurations
6. **Alert System**: Notifications for pipeline failures or warnings

## Technologies Used

- **HTML5/CSS3/JavaScript**: Modern web standards
- **Tailwind CSS**: Utility-first CSS framework
- **Chart.js**: Data visualization library
- **Font Awesome**: Icon library

## Browser Compatibility

This dashboard works on all modern browsers:
- Chrome/Chromium 80+
- Firefox 75+
- Safari 13+
- Edge 80+