import sqlite3
import os
from pathlib import Path
from flask import g, current_app

# The main.py had this logic:
# DB_PATH = os.getenv("DATABASE_PATH")
# if DB_PATH:
#     DB_PATH = Path(DB_PATH)
# else:
#     DB_PATH = Path(__file__).resolve().parent.parent / "data" / "teamsupport.db"

def get_db_path():
    db_path = os.getenv("DATABASE_PATH")
    if db_path:
        return Path(db_path)
    # Relative to this file (src/db.py), parent is src, parent parent is project root
    return Path(__file__).resolve().parent.parent / "data" / "teamsupport.db"

def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        db_path = get_db_path()
        if not db_path.exists():
            # Import logger here to avoid circularity if needed, or just raise
            raise FileNotFoundError(f"Database {db_path} not found!")
            
        # Connect in read-only mode
        g.db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        g.db.row_factory = sqlite3.Row
        
        # Attach KB database
        kb_path = db_path.parent / "kb_articles.db"
        if kb_path.exists():
            try:
                g.db.execute(f"ATTACH DATABASE '{kb_path}' AS kb")
            except sqlite3.OperationalError:
                # Silent failure or log if app context available
                pass
    return g.db

def close_db(e=None):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()
