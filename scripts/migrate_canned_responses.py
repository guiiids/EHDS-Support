"""
Canned Responses Migration Script

Migrates canned responses from Canned_Responses.xlsx into SQLite database.
Creates canned_responses table in teamsupport.db.

Usage: python migrate_canned_responses.py
"""

import sqlite3
from pathlib import Path

import pandas as pd

# Configuration
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
EXCEL_PATH = DATA_DIR / "Canned_Responses.xlsx"
DB_PATH = DATA_DIR / "teamsupport.db"


def create_table(conn: sqlite3.Connection):
    """Create the canned_responses table."""
    cursor = conn.cursor()
    
    # Drop existing table if it exists (for clean re-migration)
    cursor.execute("DROP TABLE IF EXISTS canned_responses")
    
    cursor.execute("""
        CREATE TABLE canned_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER,
            ticket_number INTEGER,
            ticket_name TEXT,
            date_created TEXT,
            action_description TEXT,
            action_type TEXT,
            category TEXT,
            parent_category TEXT,
            is_knowledgebase BOOLEAN
        )
    """)
    
    # Create indexes for faster searching
    cursor.execute("CREATE INDEX idx_cr_ticket_number ON canned_responses(ticket_number)")
    cursor.execute("CREATE INDEX idx_cr_category ON canned_responses(category)")
    cursor.execute("CREATE INDEX idx_cr_parent_category ON canned_responses(parent_category)")
    cursor.execute("CREATE INDEX idx_cr_date_created ON canned_responses(date_created)")
    
    conn.commit()
    print("✅ Created canned_responses table with indexes")


def import_data(conn: sqlite3.Connection, df: pd.DataFrame):
    """Import data from DataFrame into database."""
    cursor = conn.cursor()
    
    count = 0
    for _, row in df.iterrows():
        # Convert datetime to ISO format string
        date_created = None
        if pd.notna(row['Date Ticket Created']):
            date_created = row['Date Ticket Created'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("""
            INSERT INTO canned_responses 
            (ticket_id, ticket_number, ticket_name, date_created, action_description, 
             action_type, category, parent_category, is_knowledgebase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(row['Ticket ID']) if pd.notna(row['Ticket ID']) else None,
            int(row['Ticket Number']) if pd.notna(row['Ticket Number']) else None,
            row['Ticket Name'] if pd.notna(row['Ticket Name']) else None,
            date_created,
            row['Action Description'] if pd.notna(row['Action Description']) else None,
            row['Action Type'] if pd.notna(row['Action Type']) else None,
            row['Knowledge Base Category Name'] if pd.notna(row['Knowledge Base Category Name']) else None,
            row['Knowledge Base Parent Category Name'] if pd.notna(row['Knowledge Base Parent Category Name']) else None,
            bool(row['Is KnowledgeBase']) if pd.notna(row['Is KnowledgeBase']) else False
        ))
        count += 1
    
    conn.commit()
    print(f"✅ Imported {count} canned responses")


def main():
    """Main migration function."""
    # Check paths
    excel_path = Path(EXCEL_PATH)
    db_path = Path(DB_PATH)
    
    if not excel_path.exists():
        print(f"❌ Excel file not found: {EXCEL_PATH}")
        return False
    
    if not db_path.exists():
        print(f"❌ Database not found: {DB_PATH}")
        print("   Run the main migration first: python migrate_to_sqlite.py")
        return False
    
    print(f"📊 Reading {EXCEL_PATH}...")
    df = pd.read_excel(EXCEL_PATH, engine='openpyxl')
    print(f"   Found {len(df)} canned responses")
    
    print(f"\n💾 Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    
    try:
        create_table(conn)
        import_data(conn, df)
        
        # Verify
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM canned_responses")
        count = cursor.fetchone()[0]
        print(f"\n✅ Migration complete! Total records: {count}")
        
        # Show categories
        cursor.execute("""
            SELECT category, COUNT(*) as cnt 
            FROM canned_responses 
            GROUP BY category 
            ORDER BY cnt DESC
        """)
        print("\n📁 Categories:")
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]} responses")
        
        return True
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
