#!/usr/bin/env python3
"""
Health Check Endpoint for Cloud Deployment
Simple HTTP server to provide health status for container orchestration
"""

import http.server
import socketserver
import json
import logging
import os
from datetime import datetime
from database import db_manager
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints"""
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/health':
            self.send_health_status()
        elif self.path == '/status':
            self.send_detailed_status()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def send_health_status(self):
        """Send basic health status"""
        try:
            # Test database connection
            db_healthy = db_manager.test_connection()
            
            health_data = {
                'status': 'healthy' if db_healthy else 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'database': 'connected' if db_healthy else 'disconnected',
                'version': 'v0.9'
            }
            
            if db_healthy:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(health_data).encode())
            else:
                self.send_response(503)  # Service Unavailable
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(health_data).encode())
                
        except Exception as e:
            logger.error(f"Health check error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error_data = {
                'status': 'error',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }
            self.wfile.write(json.dumps(error_data).encode())
    
    def send_detailed_status(self):
        """Send detailed system status"""
        try:
            # Test database connection
            db_healthy = db_manager.test_connection()
            
            # Get basic stats if database is available
            stats = {}
            if db_healthy:
                try:
                    with db_manager.get_session() as session:
                        from models import Event, EventOdds, AlertLog, Result
                        stats = {
                            'events_count': session.query(Event).count(),
                            'odds_count': session.query(EventOdds).count(),
                            'alerts_count': session.query(AlertLog).count(),
                            'results_count': session.query(Result).count()
                        }
                except Exception as e:
                    logger.warning(f"Could not get stats: {e}")
                    stats = {'error': str(e)}
            
            status_data = {
                'status': 'healthy' if db_healthy else 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'database': 'connected' if db_healthy else 'disconnected',
                'version': 'v0.9',
                'environment': {
                    'proxy_enabled': Config.PROXY_ENABLED,
                    'discovery_interval': Config.DISCOVERY_INTERVAL_HOURS,
                    'poll_interval': Config.POLL_INTERVAL_MINUTES,
                    'timezone': Config.TIMEZONE
                },
                'statistics': stats
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status_data, indent=2).encode())
            
        except Exception as e:
            logger.error(f"Status check error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error_data = {
                'status': 'error',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }
            self.wfile.write(json.dumps(error_data).encode())
    
    def log_message(self, format, *args):
        """Override to use our logger instead of stderr"""
        logger.info(f"{self.address_string()} - {format % args}")

def start_health_server(port=8000):
    """Start the health check HTTP server"""
    try:
        with socketserver.TCPServer(("", port), HealthCheckHandler) as httpd:
            logger.info(f"Health check server started on port {port}")
            logger.info(f"Health endpoint: http://localhost:{port}/health")
            logger.info(f"Status endpoint: http://localhost:{port}/status")
            httpd.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")

if __name__ == '__main__':
    # Get port from environment or use default
    port = int(os.getenv('HEALTH_CHECK_PORT', '8000'))
    start_health_server(port)
