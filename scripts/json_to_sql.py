#!/usr/bin/env python3
"""
JSON to SQL Database Transformation Script
Converts Help Site JSON articles into SQLite database
"""

import json
import sqlite3
import sys
import re
from pathlib import Path
from datetime import datetime
from html.parser import HTMLParser


class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML for search indexing"""
    def __init__(self):
        super().__init__()
        self.text = []
    
    def handle_data(self, data):
        self.text.append(data.strip())
    
    def get_text(self):
        return ' '.join(self.text)


def extract_text_from_html(html_content):
    """Extract plain text from HTML content"""
    if not html_content:
        return ""
    
    parser = HTMLTextExtractor()
    try:
        parser.feed(html_content)
        return parser.get_text()
    except:
        # Fallback: simple regex-based extraction
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


def create_database(db_path):
    """Create SQLite database with help_articles table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create help_articles table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS help_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_title TEXT NOT NULL,
            breadcrumbs TEXT,
            intended_users TEXT,
            path TEXT,
            article_body TEXT,
            article_text TEXT,
            filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes for better search performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_title ON help_articles(article_title)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON help_articles(path)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_filename ON help_articles(filename)")
    
    # Create full-text search virtual table
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS help_articles_fts USING fts5(
            article_title, article_text, breadcrumbs, intended_users,
            content='help_articles',
            content_rowid='id'
        )
    """)
    
    # Create triggers to keep FTS table in sync
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS help_articles_ai AFTER INSERT ON help_articles BEGIN
            INSERT INTO help_articles_fts(rowid, article_title, article_text, breadcrumbs, intended_users)
            VALUES (new.id, new.article_title, new.article_text, new.breadcrumbs, new.intended_users);
        END
    """)
    
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS help_articles_ad AFTER DELETE ON help_articles BEGIN
            DELETE FROM help_articles_fts WHERE rowid = old.id;
        END
    """)
    
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS help_articles_au AFTER UPDATE ON help_articles BEGIN
            DELETE FROM help_articles_fts WHERE rowid = old.id;
            INSERT INTO help_articles_fts(rowid, article_title, article_text, breadcrumbs, intended_users)
            VALUES (new.id, new.article_title, new.article_text, new.breadcrumbs, new.intended_users);
        END
    """)
    
    conn.commit()
    return conn


def import_json_to_db(articles_dir, db_path):
    """Import JSON articles into SQLite database"""
    
    # Create database
    conn = create_database(db_path)
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("DELETE FROM help_articles")
    conn.commit()
    
    # Find all JSON files
    articles_path = Path(articles_dir)
    json_files = list(articles_path.glob("*.json"))
    
    if not json_files:
        print(f"Warning: No JSON files found in {articles_dir}")
        return 0, 0
    
    imported_count = 0
    skipped_count = 0
    
    for json_file in sorted(json_files):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract fields
            article_title = data.get('article_title', '').strip()
            breadcrumbs = data.get('breadcrumbs', '').strip()
            intended_users = data.get('intended_users', [])
            path = data.get('path', '').strip()
            article_body = data.get('article_body', '').strip()
            
            # Skip if no title or body
            if not article_title or not article_body:
                print(f"Skipping {json_file.name}: Missing title or body")
                skipped_count += 1
                continue
            
            # Convert intended_users list to comma-separated string
            intended_users_str = ', '.join(intended_users) if isinstance(intended_users, list) else str(intended_users)
            
            # Extract plain text from HTML for search indexing
            article_text = extract_text_from_html(article_body)
            
            # Insert into database
            cursor.execute("""
                INSERT INTO help_articles (
                    article_title,
                    breadcrumbs,
                    intended_users,
                    path,
                    article_body,
                    article_text,
                    filename
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                article_title,
                breadcrumbs,
                intended_users_str,
                path,
                article_body,
                article_text,
                json_file.name
            ))
            
            imported_count += 1
            
        except json.JSONDecodeError as e:
            print(f"Error parsing {json_file.name}: {e}")
            skipped_count += 1
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            skipped_count += 1
    
    conn.commit()
    conn.close()
    
    return imported_count, skipped_count


def main():
    """Main execution function"""
    
    # Default paths
    articles_dir = Path(__file__).parent / 'data' / 'articles'
    db_path = Path(__file__).parent / 'data' / 'help_articles.db'
    
    # Allow command-line arguments
    if len(sys.argv) > 1:
        articles_dir = Path(sys.argv[1])
    if len(sys.argv) > 2:
        db_path = Path(sys.argv[2])
    
    # Check if articles directory exists
    if not articles_dir.exists():
        print(f"Error: Articles directory not found at {articles_dir}")
        sys.exit(1)
    
    print(f"Importing JSON articles from: {articles_dir}")
    print(f"Creating database: {db_path}")
    
    # Import data
    imported, skipped = import_json_to_db(articles_dir, db_path)
    
    print(f"\n✓ Import completed successfully!")
    print(f"  - Imported: {imported} articles")
    print(f"  - Skipped: {skipped} files")
    print(f"  - Database: {db_path}")


if __name__ == "__main__":
    main()
