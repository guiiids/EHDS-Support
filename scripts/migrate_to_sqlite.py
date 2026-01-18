#!/usr/bin/env python3
"""
SQLite Migration Script for TeamSupport Archive Viewer

One-time migration from CSV files to SQLite database.
Run: python migrate_to_sqlite.py

This script:
1. Loads all CSV files
2. Filters visible rows
3. Cleans message bodies (pre-computes for runtime efficiency)
4. Creates indexed SQLite database
"""

import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# === Configuration ===
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

CSV_PATHS = [
    # 2017
    DATA_DIR / "All Ticket Actions - 01-2017_06-2017.csv",
    DATA_DIR / "All Ticket Actions - 07-2017-12-2017.csv",
    # 2018
    DATA_DIR / "All Ticket Actions - 01-2018_06-2018.csv",
    DATA_DIR / "All Ticket Actions - 07-2018_12-2018.csv",
    # 2019
    DATA_DIR / "All Ticket Actions - 01-2019_06-2019..csv",  # Note: double dot in filename
    DATA_DIR / "All Ticket Actions - 07-2019_12-2019.csv",
    # 2020
    DATA_DIR / "All Ticket Actions - 01-2020_06-2020.csv",
    DATA_DIR / "All Ticket Actions - 07-2020_12-2020.csv",
    # 2021
    DATA_DIR / "All Ticket Actions - 07-2021_12-2021.csv",
    # 2022
    DATA_DIR / "All Ticket Actions - 01-2022_06-2022.csv",
    DATA_DIR / "All Ticket Actions - 07-2022_12-2022.csv",
    # 2023
    DATA_DIR / "All Ticket Actions - 01-2023_06-2023.csv",
    DATA_DIR / "All Ticket Actions - 07-2023_12-2023.csv",
    # 2024
    DATA_DIR / "All Ticket Actions - 01-2024_06-2024.csv",
    DATA_DIR / "All Ticket Actions - 07-2024_12-2024.csv",
    # 2025
    DATA_DIR / "All-Ticket-Actions-01-2025_06-2025.csv",
    DATA_DIR / "All-Ticket-Actions-07-2025_12-2025.csv",
]

DB_PATH = DATA_DIR / "teamsupport.db"


# === Text Cleaning Functions (copied from main.py) ===

def normalize_whitespace(text):
    if not text:
        return text
    text = text.replace('\t', ' ')
    text = text.replace('\u00a0', ' ')
    text = text.replace('\u00ac\u2020', ' ')
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


def parse_portal_submission(text: str) -> str:
    pattern = r"""
        Please\s+explain\s+the\s+issue\s+you(?:'|')re\s+experiencing\s*\(with\s+as\s+much\s+detail\s+as\s+possible\)\s*:\s*
        (?P<issue>.*?)
        Location\s+where\s+issue\s+occurred\s*\(e\.g\.?\s*link,\s*name\s+of\s+core,\s*etc\.?\)\s*:\s*
        (?P<location>.*?)
        (?:\*{2}Please\s+feel\s+free\s+to\s+record.*)?$
    """
    match = re.search(pattern, text, flags=re.DOTALL | re.IGNORECASE | re.VERBOSE)
    if match:
        issue = match.group('issue').strip()
        location = match.group('location').strip()
        output_parts = []
        if issue:
            output_parts.append(f"Issue:\n{issue}")
        if location:
            output_parts.append(f"Location:\n{location}")
        return '\n\n'.join(output_parts) if output_parts else text
    return text


def clean_message_body(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text)
    
    bcc_header_pattern = r'Ticket created via e-mail \(BCC line\)\. Sender:.*?responding to requests\.\s*'
    text = re.sub(bcc_header_pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
    
    patterns = [
        r'^(Action added via e-mail|Ticket created via e-mail)\..*\n?',
        r'These people were on the To line of the email:[^\n]*\n?',
        r'These people were on the CC line of the email:[^\n]*\n?',
        r"You don't often get email from[^\n]*\n?",
        r'Learn why this is important\s*\n?',
        r'External Sender - Use caution opening files[^\n]*\n?',
        r'^Hello iLab Support,.*\n',
    ]
    
    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
    
    text = normalize_whitespace(text)
    lines = text.split('\n')
    cleaned_lines = [line.lstrip() for line in lines]
    text = '\n'.join(cleaned_lines)
    text = parse_portal_submission(text)
    
    return text.strip()


# === Migration Logic ===

def create_schema(conn: sqlite3.Connection):
    """Create database tables and indexes."""
    cursor = conn.cursor()
    
    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS messages")
    cursor.execute("DROP TABLE IF EXISTS tickets")
    
    # Create tickets table
    cursor.execute("""
        CREATE TABLE tickets (
            ticket_number INTEGER PRIMARY KEY,
            ticket_name TEXT,
            status TEXT,
            subcategory TEXT,
            date_action_created TEXT,
            date_ticket_created TEXT,
            date_closed TEXT,
            ticket_type TEXT,
            customers TEXT,
            assigned_to TEXT,
            ticket_source TEXT,
            ticket_owner TEXT
        )
    """)
    
    # Create messages table
    cursor.execute("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_number INTEGER,
            action_creator_name TEXT,
            action_type TEXT,
            date_action_created TEXT,
            action_description TEXT,
            cleaned_description TEXT,
            role TEXT,
            FOREIGN KEY (ticket_number) REFERENCES tickets(ticket_number)
        )
    """)
    
    conn.commit()
    print("✅ Schema created")


def create_indexes(conn: sqlite3.Connection):
    """Create indexes for fast queries."""
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX idx_messages_ticket ON messages(ticket_number)")
    cursor.execute("CREATE INDEX idx_tickets_status ON tickets(status)")
    cursor.execute("CREATE INDEX idx_tickets_date ON tickets(date_action_created DESC)")
    cursor.execute("CREATE INDEX idx_tickets_name ON tickets(ticket_name)")
    conn.commit()
    print("✅ Indexes created")


def migrate():
    """Main migration function."""
    print("=" * 60)
    print("🚀 TeamSupport SQLite Migration")
    print("=" * 60)
    
    # Step 1: Load CSVs
    print("\n📂 Loading CSV files...")
    dfs = []
    for i, path in enumerate(CSV_PATHS, 1):
        if not Path(path).exists():
            print(f"   ⚠️  [{i}/{len(CSV_PATHS)}] MISSING: {path}")
            continue
        print(f"   [{i}/{len(CSV_PATHS)}] Loading {path}...")
        dfs.append(pd.read_csv(path, low_memory=False))
    
    if not dfs:
        print("❌ No CSV files found! Aborting.")
        sys.exit(1)
    
    df = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Total rows loaded: {len(df):,}")
    
    # Step 2: Filter visible rows
    print("\n🔍 Filtering visible rows...")
    df = df[df['Is Visible on Hub'] == True].copy()
    print(f"   → {len(df):,} visible rows")
    
    # Step 3: Convert dates to ISO format
    print("\n📅 Converting dates...")
    for col in ['Date Action Created', 'Date Ticket Created', 'Date Closed']:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %I:%M %p', errors='coerce')
        # Convert to ISO string for SQLite
        df[col + '_ISO'] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Step 4: Clean message bodies
    print("\n🧹 Cleaning message bodies (this may take a few minutes)...")
    total = len(df)
    cleaned = []
    for i, text in enumerate(df['Action Description']):
        if i % 50000 == 0 and i > 0:
            print(f"   Processed {i:,}/{total:,} ({100*i/total:.0f}%)")
        cleaned.append(clean_message_body(text))
    df['Cleaned Description'] = cleaned
    print(f"   ✅ Cleaned {total:,} messages")
    
    # Step 5: Determine role
    print("\n👤 Determining roles...")
    df['Role'] = df.apply(
        lambda row: 'Agent' if row['Action Creator Name'] == row['Assigned To'] else 'Customer',
        axis=1
    )
    
    # Step 6: Derive ticket owner
    print("\n🎯 Deriving ticket owners...")
    def get_ticket_owner(group):
        desc_row = group[group['Action Type'] == 'Description']
        if not desc_row.empty:
            return desc_row.iloc[0]['Action Creator Name']
        return 'Unknown'
    
    ticket_owners = df.groupby('Ticket Number').apply(get_ticket_owner, include_groups=False).reset_index()
    ticket_owners.columns = ['Ticket Number', 'Ticket Owner']
    
    # Step 7: Create tickets summary
    print("\n📋 Creating tickets summary...")
    tickets_summary = df.groupby('Ticket Number').agg({
        'Ticket Name': 'first',
        'Status': 'first',
        'Subcategory': 'first',
        'Date Action Created_ISO': 'max',
        'Date Ticket Created_ISO': 'first',
        'Date Closed_ISO': 'first',
        'Ticket Type': 'first',
        'Customers': 'first',
        'Assigned To': 'first',
        'Ticket Source': 'first'
    }).reset_index()
    
    tickets_summary = tickets_summary.merge(ticket_owners, on='Ticket Number', how='left')
    tickets_summary['Ticket Owner'] = tickets_summary['Ticket Owner'].fillna('Unknown')
    
    # Rename columns for database
    tickets_summary.columns = [
        'ticket_number', 'ticket_name', 'status', 'subcategory',
        'date_action_created', 'date_ticket_created', 'date_closed',
        'ticket_type', 'customers', 'assigned_to', 'ticket_source', 'ticket_owner'
    ]
    
    print(f"   → {len(tickets_summary):,} unique tickets")
    
    # Step 8: Prepare messages for database
    print("\n💾 Preparing messages...")
    messages_df = df[['Ticket Number', 'Action Creator Name', 'Action Type', 
                      'Date Action Created_ISO', 'Action Description', 
                      'Cleaned Description', 'Role']].copy()
    messages_df.columns = [
        'ticket_number', 'action_creator_name', 'action_type',
        'date_action_created', 'action_description', 'cleaned_description', 'role'
    ]
    
    # Step 9: Write to SQLite
    print(f"\n🗄️  Writing to {DB_PATH}...")
    
    # Remove existing database
    db_file = Path(DB_PATH)
    if db_file.exists():
        db_file.unlink()
        print("   Removed existing database")
    
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    
    # Insert tickets
    tickets_summary.to_sql('tickets', conn, if_exists='append', index=False)
    print(f"   ✅ Inserted {len(tickets_summary):,} tickets")
    
    # Insert messages
    messages_df.to_sql('messages', conn, if_exists='append', index=False)
    print(f"   ✅ Inserted {len(messages_df):,} messages")
    
    create_indexes(conn)
    
    # Optimize database
    conn.execute("VACUUM")
    conn.execute("ANALYZE")
    
    conn.close()
    
    # Report file size
    db_size = db_file.stat().st_size / (1024 * 1024)
    print(f"\n📁 Database file: {DB_PATH} ({db_size:.1f} MB)")
    
    print("\n" + "=" * 60)
    print("✅ Migration complete!")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   • Tickets: {len(tickets_summary):,}")
    print(f"   • Messages: {len(messages_df):,}")
    print(f"   • Database size: {db_size:.1f} MB")
    print(f"\nNext steps:")
    print(f"   1. The refactored main.py will use {DB_PATH}")
    print(f"   2. Start the app: python main.py")


def verify():
    """Verify the database integrity."""
    print("🔍 Verifying database...")
    
    if not Path(DB_PATH).exists():
        print(f"❌ Database {DB_PATH} not found!")
        sys.exit(1)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Count tickets
    cursor.execute("SELECT COUNT(*) FROM tickets")
    ticket_count = cursor.fetchone()[0]
    
    # Count messages
    cursor.execute("SELECT COUNT(*) FROM messages")
    message_count = cursor.fetchone()[0]
    
    # Sample ticket
    cursor.execute("SELECT * FROM tickets ORDER BY date_action_created DESC LIMIT 1")
    sample = cursor.fetchone()
    
    conn.close()
    
    print(f"✅ Database verified:")
    print(f"   • Tickets: {ticket_count:,}")
    print(f"   • Messages: {message_count:,}")
    print(f"   • Latest ticket: #{sample[0]} - {sample[1][:50]}...")


if __name__ == '__main__':
    if '--verify' in sys.argv:
        verify()
    else:
        migrate()
