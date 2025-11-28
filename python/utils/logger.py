"""
Enhanced Logger for Multi-Node Transaction Testing
Provides comprehensive logging and reporting capabilities
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

class TransactionLogger:
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        self.test_results = {}
        self.current_test = None
        self.test_start_time = None
        
    def start_test_case(self, test_name: str, isolation_level: str, 
                       node_pairs: List[int], trans_ids: List[int]):
        """Start logging for a specific test case"""
        self.current_test = {
            'test_name': test_name,
            'isolation_level': isolation_level,
            'node_pairs': node_pairs,
            'trans_ids': trans_ids,
            'start_time': datetime.now(),
            'transactions': [],
            'results': {},
            'anomalies': [],
            'summary': {}
        }
        self.test_start_time = time.time()
        
        print(f"\n{'='*80}")
        print(f"STARTING TEST CASE: {test_name}")
        print(f"Isolation Level: {isolation_level}")
        print(f"Node Pairs: {node_pairs}")
        print(f"Transaction IDs: {trans_ids}")
        print(f"Start Time: {self.current_test['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
    
    def log_transaction_step(self, step_name: str, node_id: int, operation: str, 
                           query_result: Dict[str, Any], notes: str = ""):
        """Log individual transaction step"""
        if not self.current_test:
            return
            
        step_data = {
            'step_name': step_name,
            'node_id': node_id,
            'operation': operation,
            'timestamp': query_result.get('timestamp', datetime.now().strftime('%H:%M:%S.%f')[:-3]),
            'success': query_result.get('success', False),
            'duration': query_result.get('duration', 0),
            'row_count': query_result.get('row_count', 0),
            'results': query_result.get('results', []),
            'error': query_result.get('error'),
            'notes': notes
        }
        
        self.current_test['transactions'].append(step_data)
        
        # Print step info
        status = "PASS" if step_data['success'] else "FAIL"
        print(f"{status} [{step_data['timestamp']}] Node {node_id} | {step_name}")
        print(f"   Operation: {operation}")
        print(f"   Duration: {step_data['duration']}s")
        
        if step_data['success']:
            if step_data['results']:
                print(f"   Results: {len(step_data['results'])} rows")
                for result in step_data['results'][:3]:  # Show first 3 results
                    print(f"      {result}")
                if len(step_data['results']) > 3:
                    print(f"      ... and {len(step_data['results']) - 3} more rows")
            elif step_data['row_count'] > 0:
                print(f"   Affected rows: {step_data['row_count']}")
        else:
            print(f"   Error: {step_data['error']}")
            
        if notes:
            print(f"   Notes: {notes}")
        print()
    
    def log_anomaly(self, anomaly_type: str, description: str, 
                   expected_data: Any = None, actual_data: Any = None):
        """Log concurrency anomaly detection"""
        if not self.current_test:
            return
            
        anomaly = {
            'type': anomaly_type,
            'description': description,
            'expected_data': expected_data,
            'actual_data': actual_data,
            'timestamp': datetime.now().strftime('%H:%M:%S.%f')[:-3]
        }
        
        self.current_test['anomalies'].append(anomaly)
        
        print(f"ANOMALY DETECTED: {anomaly_type}")
        print(f"   Description: {description}")
        if expected_data is not None:
            print(f"   Expected: {expected_data}")
        if actual_data is not None:
            print(f"   Actual: {actual_data}")
        print()
    
    def end_test_case(self, final_results: Dict[str, Any] = None):
        """End current test case and generate summary"""
        if not self.current_test:
            return
            
        end_time = time.time()
        total_duration = round(end_time - self.test_start_time, 4)
        
        self.current_test['end_time'] = datetime.now()
        self.current_test['total_duration'] = total_duration
        
        # Generate summary
        summary = {
            'total_steps': len(self.current_test['transactions']),
            'successful_steps': len([t for t in self.current_test['transactions'] if t['success']]),
            'failed_steps': len([t for t in self.current_test['transactions'] if not t['success']]),
            'total_anomalies': len(self.current_test['anomalies']),
            'total_duration': total_duration,
            'avg_step_duration': round(sum(t['duration'] for t in self.current_test['transactions']) / 
                                     max(len(self.current_test['transactions']), 1), 4)
        }
        
        if final_results:
            summary.update(final_results)
            
        self.current_test['summary'] = summary
        
        # Store test results
        test_key = f"{self.current_test['test_name']}_{self.current_test['isolation_level']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.test_results[test_key] = self.current_test.copy()
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"TEST CASE COMPLETED: {self.current_test['test_name']}")
        print(f"Isolation Level: {self.current_test['isolation_level']}")
        print(f"Duration: {total_duration}s")
        print(f"\nSUMMARY:")
        print(f"   Total Steps: {summary['total_steps']}")
        print(f"   Successful: {summary['successful_steps']}")
        print(f"   Failed: {summary['failed_steps']}")
        print(f"   Anomalies: {summary['total_anomalies']}")
        print(f"   Avg Step Duration: {summary['avg_step_duration']}s")
        
        if self.current_test['anomalies']:
            print(f"\nANOMALIES DETECTED:")
            for anomaly in self.current_test['anomalies']:
                print(f"   - {anomaly['type']}: {anomaly['description']}")
        else:
            print(f"\nNO ANOMALIES DETECTED")
            
        if final_results:
            print(f"\nFINAL RESULTS:")
            for key, value in final_results.items():
                print(f"   {key}: {value}")
        
        print(f"{'='*80}\n")
        
        # Save to file
        self._save_test_to_file(test_key)
        
        self.current_test = None
        self.test_start_time = None
    
    def _save_test_to_file(self, test_key: str):
        """Save test results to JSON file"""
        filename = self.log_dir / f"{test_key}.json"
        
        # Convert datetime objects to strings for JSON serialization
        test_data = self.test_results[test_key].copy()
        test_data['start_time'] = test_data['start_time'].isoformat()
        test_data['end_time'] = test_data['end_time'].isoformat()
        
        try:
            with open(filename, 'w') as f:
                json.dump(test_data, f, indent=2, default=str)
            print(f"Test results saved to: {filename}")
        except Exception as e:
            print(f"Failed to save test results: {e}")
    
    def generate_isolation_level_report(self, isolation_level: str):
        """Generate comprehensive report for specific isolation level"""
        matching_tests = {k: v for k, v in self.test_results.items() 
                         if v['isolation_level'] == isolation_level}
        
        if not matching_tests:
            print(f"No test results found for isolation level: {isolation_level}")
            return
            
        print(f"\n{'='*100}")
        print(f"ISOLATION LEVEL REPORT: {isolation_level}")
        print(f"{'='*100}")
        
        total_tests = len(matching_tests)
        total_anomalies = sum(len(test['anomalies']) for test in matching_tests.values())
        total_duration = sum(test['total_duration'] for test in matching_tests.values())
        
        print(f"\nOVERVIEW:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Total Anomalies: {total_anomalies}")
        print(f"   Total Duration: {round(total_duration, 2)}s")
        print(f"   Avg Test Duration: {round(total_duration/total_tests, 2)}s")
        
        print(f"\nTEST DETAILS:")
        for test_name, test_data in matching_tests.items():
            print(f"\n   Test: {test_data['test_name']}")
            print(f"      Duration: {test_data['total_duration']}s")
            print(f"      Steps: {test_data['summary']['total_steps']}")
            print(f"      Anomalies: {len(test_data['anomalies'])}")
            
            if test_data['anomalies']:
                for anomaly in test_data['anomalies']:
                    print(f"         - {anomaly['type']}: {anomaly['description']}")
        
        # Save report to file
        report_filename = self.log_dir / f"isolation_report_{isolation_level.lower().replace(' ', '_')}.txt"
        try:
            with open(report_filename, 'w') as f:
                f.write(f"ISOLATION LEVEL REPORT: {isolation_level}\n")
                f.write("="*50 + "\n\n")
                f.write(f"Total Tests: {total_tests}\n")
                f.write(f"Total Anomalies: {total_anomalies}\n")
                f.write(f"Total Duration: {round(total_duration, 2)}s\n")
                f.write(f"Average Test Duration: {round(total_duration/total_tests, 2)}s\n\n")
                
                for test_name, test_data in matching_tests.items():
                    f.write(f"\nTest: {test_data['test_name']}\n")
                    f.write(f"Duration: {test_data['total_duration']}s\n")
                    f.write(f"Steps: {test_data['summary']['total_steps']}\n")
                    f.write(f"Anomalies: {len(test_data['anomalies'])}\n")
                    
                    for anomaly in test_data['anomalies']:
                        f.write(f"  - {anomaly['type']}: {anomaly['description']}\n")
            
            print(f"\nReport saved to: {report_filename}")
        except Exception as e:
            print(f"Failed to save report: {e}")
    
    def generate_comprehensive_report(self):
        """Generate comprehensive report for all tests"""
        if not self.test_results:
            print("No test results available")
            return
            
        print(f"\n{'='*120}")
        print(f"COMPREHENSIVE TEST REPORT")
        print(f"{'='*120}")
        
        # Group by isolation level
        by_isolation = {}
        for test_name, test_data in self.test_results.items():
            level = test_data['isolation_level']
            if level not in by_isolation:
                by_isolation[level] = []
            by_isolation[level].append(test_data)
        
        print(f"\nISOLATION LEVEL COMPARISON:")
        print(f"{'Isolation Level':<20} {'Tests':<8} {'Anomalies':<12} {'Avg Duration':<15} {'Success Rate':<12}")
        print(f"{'-'*70}")
        
        for level, tests in by_isolation.items():
            total_tests = len(tests)
            total_anomalies = sum(len(test['anomalies']) for test in tests)
            avg_duration = round(sum(test['total_duration'] for test in tests) / total_tests, 2)
            successful_tests = len([test for test in tests if len(test['anomalies']) == 0])
            success_rate = round((successful_tests / total_tests) * 100, 1)
            
            print(f"{level:<20} {total_tests:<8} {total_anomalies:<12} {avg_duration:<15} {success_rate}%")
        
        # Save comprehensive report
        report_filename = self.log_dir / f"comprehensive_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(report_filename, 'w') as f:
                f.write("COMPREHENSIVE TEST REPORT\n")
                f.write("="*50 + "\n\n")
                f.write("ISOLATION LEVEL COMPARISON:\n")
                f.write(f"{'Isolation Level':<20} {'Tests':<8} {'Anomalies':<12} {'Avg Duration':<15} {'Success Rate':<12}\n")
                f.write(f"{'-'*70}\n")
                
                for level, tests in by_isolation.items():
                    total_tests = len(tests)
                    total_anomalies = sum(len(test['anomalies']) for test in tests)
                    avg_duration = round(sum(test['total_duration'] for test in tests) / total_tests, 2)
                    successful_tests = len([test for test in tests if len(test['anomalies']) == 0])
                    success_rate = round((successful_tests / total_tests) * 100, 1)
                    
                    f.write(f"{level:<20} {total_tests:<8} {total_anomalies:<12} {avg_duration:<15} {success_rate}%\n")
            
            print(f"\nComprehensive report saved to: {report_filename}")
        except Exception as e:
            print(f"Failed to save comprehensive report: {e}")
