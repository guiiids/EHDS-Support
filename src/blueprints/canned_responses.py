from flask import Blueprint, render_template, request, redirect, url_for, g, jsonify
import sqlite3
from pathlib import Path
from datetime import datetime

# Define the Blueprint
bp = Blueprint('canned_responses', __name__, url_prefix='/canned-responses')

# Database Path (relative to this file: src/blueprints/canned_responses.py -> src/../data/kb_articles.db)
DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "kb_articles.db"

def get_db_connection():
    """Create database connection to KB database"""
    # Check if we already have a connection for this request
    if 'kb_db' not in g:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"KB Database not found at {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        g.kb_db = conn
    return g.kb_db

def format_date(date_str):
    """Format date string for display"""
    if not date_str:
        return 'N/A'
    
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%b %d, %Y")
    except ValueError:
        return date_str

def get_categories():
    """Get all unique categories from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT category_name FROM kb_articles WHERE category_name IS NOT NULL AND category_name != '' ORDER BY category_name")
    categories = [row['category_name'] for row in cursor.fetchall()]
    return categories

@bp.teardown_app_request
def close_db(exception):
    """Close the database connection at the end of the request"""
    db = g.pop('kb_db', None)
    if db is not None:
        db.close()

@bp.route('/')
def index():
    """Display list of KB articles (Default View)"""
    return kb_list()

@bp.route('/list')
def kb_list():
    """Display list of KB articles with search, filter, and sort"""
    
    # Get query parameters
    search_query = request.args.get('q', '').strip()
    filter_category = request.args.get('category', '').strip()
    sort_by = request.args.get('sort', 'date_modified')
    sort_order = request.args.get('order', 'desc')
    
    # Validate sort parameters
    valid_sort_fields = ['ticket_number', 'title', 'author', 'date_modified', 'date_created']
    if sort_by not in valid_sort_fields:
        sort_by = 'date_modified'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build query
    query = "SELECT * FROM kb_articles WHERE 1=1"
    params = []
    
    # Apply search filter using FTS
    if search_query:
        query = """
            SELECT kb_articles.* FROM kb_articles
            INNER JOIN kb_articles_fts ON kb_articles.id = kb_articles_fts.rowid
            WHERE kb_articles_fts MATCH ?
        """
        params.append(search_query)
        
        # Add category filter if present
        if filter_category:
            query += " AND kb_articles.category_name = ?"
            params.append(filter_category)
    else:
        # Apply category filter
        if filter_category:
            query += " AND category_name = ?"
            params.append(filter_category)
    
    # Apply sorting
    query += f" ORDER BY {sort_by} {sort_order.upper()}"
    
    # Add limit for initial load (lazy loading)
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    query += f" LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    # Execute query
    cursor.execute(query, params)
    articles = cursor.fetchall()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) as count FROM kb_articles")
    result = cursor.fetchone()
    total_count = result['count'] if result else 0
    
    # Format dates for display
    articles_list = []
    for article in articles:
        article_dict = dict(article)
        article_dict['date_modified_display'] = format_date(article_dict.get('date_modified'))
        article_dict['date_created_display'] = format_date(article_dict.get('date_created'))
        articles_list.append(article_dict)
    
    # Get categories for filter dropdown
    categories = get_categories()
    
    return render_template(
        'kb_list.html',
        articles=articles_list,
        search_query=search_query,
        filter_category=filter_category,
        sort_by=sort_by,
        sort_order=sort_order,
        filtered_count=len(articles_list),
        total_count=total_count,
        categories=categories
    )

@bp.route('/<int:article_id>')
def kb_detail(article_id):
    """Display single KB article detail"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM kb_articles WHERE id = ?", (article_id,))
    article = cursor.fetchone()
    
    if not article:
        return "Article not found", 404
    
    # Convert to dict and format dates
    article_dict = dict(article)
    article_dict['date_modified_display'] = format_date(article_dict.get('date_modified'))
    article_dict['date_created_display'] = format_date(article_dict.get('date_created'))
    
    return render_template('kb_detail.html', article=article_dict)

@bp.route('/api/articles')
def api_articles():
    """API endpoint for fetching articles (used for lazy loading)"""
    search_query = request.args.get('q', '').strip()
    filter_category = request.args.get('category', '').strip()
    sort_by = request.args.get('sort', 'date_modified')
    sort_order = request.args.get('order', 'desc')
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    # Validate sort parameters
    valid_sort_fields = ['ticket_number', 'title', 'author', 'date_modified', 'date_created']
    if sort_by not in valid_sort_fields:
        sort_by = 'date_modified'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build query
    query = "SELECT * FROM kb_articles WHERE 1=1"
    params = []
    
    # Apply search filter
    if search_query:
        query = """
            SELECT kb_articles.* FROM kb_articles
            INNER JOIN kb_articles_fts ON kb_articles.id = kb_articles_fts.rowid
            WHERE kb_articles_fts MATCH ?
        """
        params.append(search_query)
        if filter_category:
            query += " AND kb_articles.category_name = ?"
            params.append(filter_category)
    else:
        if filter_category:
            query += " AND category_name = ?"
            params.append(filter_category)
    
    query += f" ORDER BY {sort_by} {sort_order.upper()}"
    query += f" LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    articles = cursor.fetchall()
    
    articles_list = []
    for article in articles:
        article_dict = dict(article)
        article_dict['date_modified_display'] = format_date(article_dict.get('date_modified'))
        article_dict['date_created_display'] = format_date(article_dict.get('date_created'))
        # Include detail URL for frontend
        article_dict['detail_url'] = url_for('canned_responses.kb_detail', article_id=article_dict['id'])
        articles_list.append(article_dict)
    
    return jsonify({
        'articles': articles_list,
        'count': len(articles_list),
        'offset': offset,
        'limit': limit
    })

@bp.route('/search')
def search():
    """Search endpoint - redirects to kb_list with query"""
    query = request.args.get('q', '')
    return redirect(url_for('canned_responses.kb_list', q=query))
