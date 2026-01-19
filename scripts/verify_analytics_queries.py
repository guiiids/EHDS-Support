import sqlite3
import os
import sys
from pathlib import Path

# Add src to path for imports if needed, but here we can just use sqlite3 directly
DB_PATH = Path("/Users/vieirama/iLab-JSD/TeamSupport/data/teamsupport.db")

def verify_queries():
    print(f"Verifying analytics queries on {DB_PATH}...")
    
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)
        
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    queries = {
        "Total Customers": "SELECT COUNT(DISTINCT customers) FROM tickets WHERE customers IS NOT NULL AND customers != ''",
        "Active Customers (30d)": "SELECT COUNT(DISTINCT customers) FROM tickets WHERE customers IS NOT NULL AND customers != '' AND date_ticket_created >= date('now', '-30 days')",
        "Avg Response Time": """
            SELECT AVG(CAST((julianday(m.date_action_created) - julianday(t.date_ticket_created)) * 24 AS REAL))
            FROM tickets t
            JOIN messages m ON t.ticket_number = m.ticket_number
            WHERE m.role = 'Agent' AND m.action_type != 'Description'
        """,
        "Avg Resolution Time": """
            SELECT AVG(CAST((julianday(date_closed) - julianday(date_ticket_created)) * 24 AS REAL))
            FROM tickets
            WHERE date_closed IS NOT NULL AND status IN ('Closed', 'Resolved')
        """,
        "Tickets By Customer": """
            SELECT customers, COUNT(*) as ticket_count
            FROM tickets
            WHERE customers IS NOT NULL AND customers != ''
            GROUP BY customers
            ORDER BY ticket_count DESC
            LIMIT 5
        """,
        "Churn Risk": """
            SELECT customers, MAX(date_ticket_created) as last_ticket, CAST((julianday('now') - julianday(MAX(date_ticket_created))) AS INTEGER) as days_idle
            FROM tickets
            WHERE customers IS NOT NULL AND customers != ''
            GROUP BY customers
            HAVING days_idle > 90
            ORDER BY days_idle DESC
            LIMIT 5
        """,
        "Category Breakdown": "SELECT ticket_type, COUNT(*) FROM tickets GROUP BY ticket_type LIMIT 5",
        "Source Distribution": "SELECT ticket_source, COUNT(*) FROM tickets GROUP BY ticket_source",
        "Loyalty Segments": """
            WITH stats AS (SELECT customers, COUNT(*) as c FROM tickets GROUP BY customers)
            SELECT CASE WHEN c=1 THEN 'New' ELSE 'Return' END as s, COUNT(*) FROM stats GROUP BY s
        """
    }
    
    for name, sql in queries.items():
        try:
            print(f"Testing {name}...", end=" ", flush=True)
            cursor.execute(sql)
            res = cursor.fetchone()
            print("OK.")
            # print(f"  Result: {dict(res) if res and hasattr(res, 'keys') else res[0]}")
        except Exception as e:
            print(f"FAILED: {e}")
            
    conn.close()
    print("\nVerification complete.")

if __name__ == "__main__":
    verify_queries()
