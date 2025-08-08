#!/usr/bin/env python3
"""
Health check script for transit pipeline components
"""
import requests
import json
import sys
from datetime import datetime

def check_service(name, url, expected_status=200):
    """Check if a service is responding"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == expected_status:
            return {"service": name, "status": "healthy", "response_time": response.elapsed.total_seconds()}
        else:
            return {"service": name, "status": "unhealthy", "error": f"Status {response.status_code}"}
    except Exception as e:
        return {"service": name, "status": "unhealthy", "error": str(e)}

def main():
    """Run health checks on all services"""
    services = [
        ("Kafka UI", "http://localhost:8083"),
        ("Flink JobManager", "http://localhost:8081"),
        ("Spark Master", "http://localhost:8082"),
        ("Airflow", "http://localhost:8089/health"),
        ("Jupyter", "http://localhost:8888"),
        ("Hive Server", "http://localhost:10002")  # Web UI
    ]
    
    results = []
    for name, url in services:
        result = check_service(name, url)
        results.append(result)
        
        # Print immediate feedback
        status_emoji = "✅" if result["status"] == "healthy" else "❌"
        print(f"{status_emoji} {result['service']}: {result['status']}")
        if "error" in result:
            print(f"   Error: {result['error']}")
        elif "response_time" in result:
            print(f"   Response time: {result['response_time']:.3f}s")
    
    # Summary
    healthy_count = sum(1 for r in results if r["status"] == "healthy")
    total_count = len(results)
    
    print(f"\n📊 Health Summary: {healthy_count}/{total_count} services healthy")
    
    if healthy_count == total_count:
        print("🎉 All systems operational!")
        sys.exit(0)
    else:
        print("⚠️  Some services need attention")
        sys.exit(1)

if __name__ == "__main__":
    print(f"🔍 Transit Pipeline Health Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    main()