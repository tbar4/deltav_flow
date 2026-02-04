#!/usr/bin/env python3
"""
Simple HTTP server for the DeltaV Flow HTMX frontend
"""

import http.server
import socketserver
import json
from urllib.parse import urlparse, parse_qs
import os

PORT = 8000

class HTMXHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Parse the URL
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        # API endpoints
        if path.startswith('/api/'):
            self.handle_api_request(path)
            return
            
        # Serve static files
        super().do_GET()
    
    def handle_api_request(self, path):
        """Handle API requests for HTMX"""
        if path == '/api/dashboard':
            self.serve_file('dashboard.html')
        elif path == '/api/pipelines':
            # Just return the pipeline section
            with open('dashboard.html', 'r') as f:
                content = f.read()
                # Extract pipeline section
                start = content.find('<div id="pipelines-container">')
                end = content.find('<!-- Recent Activity -->')
                if start != -1 and end != -1:
                    pipeline_section = content[start:end]
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(pipeline_section.encode())
                else:
                    self.send_error(404, "Pipeline section not found")
        elif path.startswith('/api/pipeline/details/'):
            self.serve_file('pipeline-details.html')
        elif path == '/api/activity':
            # Return just the activity list
            with open('dashboard.html', 'r') as f:
                content = f.read()
                start = content.find('<div id="activity-list">')
                end = content.find('</div>', content.find('<!-- Recent Activity -->'))
                if start != -1 and end != -1:
                    activity_section = content[start:end+6]
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(activity_section.encode())
                else:
                    self.send_error(404, "Activity section not found")
        elif path == '/api/refresh':
            self.serve_file('dashboard.html')
        elif path == '/api/close-modal':
            # Return empty content to close modal
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'')
        else:
            self.send_error(404, "API endpoint not found")
    
    def serve_file(self, filepath):
        """Serve a file with proper headers"""
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                content = f.read()
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(content.encode())
        else:
            self.send_error(404, f"File {filepath} not found")

def run_server():
    """Start the HTTP server"""
    with socketserver.TCPServer(("", PORT), HTMXHandler) as httpd:
        print(f"Serving HTMX frontend at http://localhost:{PORT}")
        print("Press Ctrl+C to stop the server")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            httpd.shutdown()

if __name__ == "__main__":
    # Change to the directory where this script is located
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    run_server()