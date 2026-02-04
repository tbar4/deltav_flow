#!/bin/bash

# Script to start the HTMX frontend server

echo "Starting DeltaV Flow HTMX Frontend Server..."
echo "Navigate to http://localhost:8000 in your browser"

# Make sure we're in the correct directory
cd "$(dirname "$0")"

# Start the server
python3 server.py