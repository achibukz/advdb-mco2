"""
Server Ping Utility - Check database node connectivity
"""

import threading
import time
from datetime import datetime
from python.db.db_config import get_db_connection


class NodePinger:
    """Background thread that pings database nodes every 5 seconds"""

    def __init__(self, interval=5):
        """
        Initialize the node pinger

        Args:
            interval: Time in seconds between pings (default: 5)
        """
        self.interval = interval
        self.running = False
        self.thread = None
        self.node_status = {1: False, 2: False, 3: False}

    def check_node(self, node):
        """
        Check if a specific node is online

        Args:
            node: Node number (1, 2, or 3)

        Returns:
            bool: True if node is online, False otherwise
        """
        try:
            conn = get_db_connection(node)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False

    def ping_all_nodes(self):
        """Check all nodes and print status"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        previous_status = self.node_status.copy()

        # Check each node
        for node in [1, 2, 3]:
            self.node_status[node] = self.check_node(node)

        # Detect nodes that came back online
        recovered_nodes = []
        for node in [1, 2, 3]:
            if not previous_status.get(node, False) and self.node_status[node]:
                recovered_nodes.append(node)
                print(f"Node {node} came back online - checking for recovery logs...")
                
                # Trigger recovery check for this node
                self._check_recovery_for_node(node)

        # Build status message
        online_nodes = [node for node, status in self.node_status.items() if status]
        offline_nodes = [node for node, status in self.node_status.items() if not status]

        print(f"\n[{timestamp}] Node Status Check:")
        if online_nodes:
            print(f"  Online: Node {', Node '.join(map(str, online_nodes))}")
        if offline_nodes:
            print(f"  Offline: Node {', Node '.join(map(str, offline_nodes))}")
        
        # Store recovery notifications for UI
        if recovered_nodes:
            if not hasattr(self, 'recovery_notifications'):
                self.recovery_notifications = []
            
            for node in recovered_nodes:
                self.recovery_notifications.append({
                    'node': node,
                    'timestamp': timestamp,
                    'message': f'Node {node} recovered and processed pending transactions'
                })

        return self.node_status

    def _ping_loop(self):
        """Background loop that pings nodes at regular intervals"""
        while self.running:
            try:
                self.ping_all_nodes()
            except Exception as e:
                print(f"Error during node ping: {e}")

            # Sleep for the interval
            time.sleep(self.interval)

    def start(self):
        """Start the background ping thread"""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._ping_loop, daemon=True)
        self.thread.start()
        print(f"Node pinger started (checking every {self.interval} seconds)")

    def stop(self):
        """Stop the background ping thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=self.interval + 1)
        print("Node pinger stopped")

    def _check_recovery_for_node(self, node_id):
        """Check and process recovery logs for a node that came back online"""
        try:
            from python.utils.recovery_manager import RecoveryManager
            from python.db.db_config import get_node_config
            
            # Get node configuration
            node_config = get_node_config(node_id)
            
            # Initialize recovery manager for this node
            recovery_manager = RecoveryManager(node_config, node_id)
            
            # Check for pending recovery logs
            print(f"Checking recovery logs for Node {node_id}...")
            recovery_results = recovery_manager.check_and_recover_pending_logs()
            
            if recovery_results['total_logs'] > 0:
                print(f"Recovery Results for Node {node_id}:")
                print(f"   Total logs found: {recovery_results['total_logs']}")
                print(f"   Successfully recovered: {recovery_results['recovered']}")
                print(f"   Failed recoveries: {recovery_results['failed']}")
                print(f"   Skipped (duplicates): {recovery_results['skipped']}")
                
                if recovery_results['recovered'] > 0:
                    print(f"Node {node_id} successfully processed {recovery_results['recovered']} pending transaction(s)!")
                elif recovery_results['failed'] > 0:
                    print(f"Node {node_id} had {recovery_results['failed']} failed recovery attempt(s)")
                
                # Store detailed recovery info for UI
                if not hasattr(self, 'recovery_details'):
                    self.recovery_details = {}
                
                self.recovery_details[node_id] = {
                    'timestamp': datetime.now().isoformat(),
                    'results': recovery_results,
                    'success': recovery_results['recovered'] > 0
                }
            else:
                print(f"No pending recovery logs found for Node {node_id}")
                
        except Exception as e:
            print(f"Error during recovery check for Node {node_id}: {str(e)}")
            
    def get_status(self):
        """
        Get current node status

        Returns:
            dict: Node status dictionary {node_id: is_online}
        """
        return self.node_status.copy()
    
    def get_recovery_notifications(self):
        """Get and clear recovery notifications for UI display"""
        if hasattr(self, 'recovery_notifications'):
            notifications = self.recovery_notifications.copy()
            self.recovery_notifications.clear()  # Clear after reading
            return notifications
        return []
    
    def get_recovery_details(self):
        """Get detailed recovery information"""
        if hasattr(self, 'recovery_details'):
            return self.recovery_details
        return {}

