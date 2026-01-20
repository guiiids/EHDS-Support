from flask import Blueprint, render_template, request, redirect, url_for, g, jsonify
import sqlite3
from pathlib import Path
from datetime import datetime
import re

# Define the Blueprint
bp = Blueprint('help_articles', __name__, url_prefix='/help-articles')

# Database Path (relative to this file: src/blueprints/help_articles.py -> src/../data/help_articles.db)
DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "help_articles.db"

def get_db_connection():
    """Create database connection to Help Articles database"""
    # Check if we already have a connection for this request
    if 'help_db' not in g:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"Help Articles Database not found at {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        g.help_db = conn
    return g.help_db

def extract_category_from_breadcrumbs(breadcrumbs):
    """Extract main category from breadcrumbs"""
    if not breadcrumbs:
        return None
    
    # Split by > and get the second part (after "Support Home")
    parts = [p.strip() for p in breadcrumbs.split('>')]
    if len(parts) > 1:
        return parts[1]
    return None

def build_navigation():
    """Build navigation structure from articles grouped by category"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, article_title, breadcrumbs
        FROM help_articles
        ORDER BY breadcrumbs, article_title
    """)
    articles = cursor.fetchall()
    
    # Group articles by category
    categories = {}
    for article in articles:
        category = extract_category_from_breadcrumbs(article['breadcrumbs'])
        if not category:
            category = "General"
        
        if category not in categories:
            categories[category] = []
        
        categories[category].append({
            'title': article['article_title'],
            'href': url_for('help_articles.docs_article', article_id=article['id'])
        })
    
    # Convert to navigation structure
    navigation = []
    for category, links in sorted(categories.items()):
        navigation.append({
            'title': category,
            'links': links
        })
    
    return navigation

def generate_article_slug(article):
    """
    Generate SEO-friendly URL slug from article breadcrumbs and title.
    Format: /help-articles/Category/Subcategory/Article_Title-{id}
    
    Args:
        article: dict-like object with 'breadcrumbs', 'article_title', and 'id' keys
    
    Returns:
        str: URL slug (e.g., "Managing_an_Institution/Institution_Dashboard-5")
    """
    slug_parts = []
    
    # Parse breadcrumbs (skip "Support Home")
    if article.get('breadcrumbs'):
        breadcrumb_parts = [p.strip() for p in article['breadcrumbs'].split('>')]
        # Skip first element if it's "Support Home"
        for part in breadcrumb_parts[1:] if len(breadcrumb_parts) > 1 else breadcrumb_parts:
            if part:
                slug_parts.append(sanitize_slug_part(part))
    
    # Add article title
    title = article.get('article_title', 'Untitled')
    slug_parts.append(sanitize_slug_part(title))
    
    # Join parts and append ID
    slug = '/'.join(slug_parts)
    slug = f"{slug}-{article['id']}"
    
    return slug

def sanitize_slug_part(text):
    """
    Sanitize a single part of the URL slug.
    Replace spaces with underscores, remove/replace special characters.
    """
    if not text:
        return ""
    
    # Replace spaces with underscores
    text = text.replace(' ', '_')
    
    # Replace problematic characters
    text = text.replace('/', '-')
    text = text.replace('\\', '-')
    
    # Remove characters that are not alphanumeric, underscore, or hyphen
    text = re.sub(r'[^\w\-]', '', text)
    
    return text

@bp.teardown_app_request
def close_db(exception):
    """Close the database connection at the end of the request"""
    db = g.pop('help_db', None)
    if db is not None:
        db.close()

@bp.route('/')
def index():
    """Redirect to help list (Default View)"""
    return redirect(url_for('help_articles.help_list'))

@bp.route('/list')
def help_list():
    """Display list of help articles with search and sort"""
    
    # Get query parameters
    search_query = request.args.get('q', '').strip()
    sort_by = request.args.get('sort', 'id')
    sort_order = request.args.get('order', 'asc')
    
    # Validate sort parameters
    valid_sort_fields = ['id', 'article_title', 'breadcrumbs']
    if sort_by not in valid_sort_fields:
        sort_by = 'id'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'asc'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build query
    query = "SELECT * FROM help_articles WHERE 1=1"
    params = []
    
    # Apply search filter using FTS
    if search_query:
        query = """
            SELECT help_articles.* FROM help_articles
            INNER JOIN help_articles_fts ON help_articles.id = help_articles_fts.rowid
            WHERE help_articles_fts MATCH ?
        """
        params.append(search_query)
    
    # Apply sorting
    query += f" ORDER BY {sort_by} {sort_order.upper()}"
    
    # Execute query
    cursor.execute(query, params)
    articles = cursor.fetchall()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) as count FROM help_articles")
    total_count = cursor.fetchone()['count']
    
    # Process articles for display
    articles_list = []
    for article in articles:
        article_dict = dict(article)
        
        # Extract category from breadcrumbs
        article_dict['category'] = extract_category_from_breadcrumbs(article_dict.get('breadcrumbs'))
        
        # Parse intended users
        intended_users = article_dict.get('intended_users', '')
        if intended_users:
            article_dict['intended_users_list'] = [u.strip() for u in intended_users.split(',') if u.strip()]
        else:
            article_dict['intended_users_list'] = []
        
        articles_list.append(article_dict)
    
    return render_template(
        'help_list.html',
        articles=articles_list,
        search_query=search_query,
        sort_by=sort_by,
        sort_order=sort_order,
        filtered_count=len(articles_list),
        total_count=total_count
    )

@bp.route('/<path:article_slug>')
def help_detail(article_slug):
    """
    Display single help article detail using slug-based URL.
    Extracts article ID from end of slug (format: .../Article_Title-{id})
    """
    
    # Extract article ID from end of slug using regex
    match = re.search(r'-(\d+)$', article_slug)
    if not match:
        return "Article not found", 404
    
    article_id = int(match.group(1))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM help_articles WHERE id = ?", (article_id,))
    article = cursor.fetchone()
    
    if not article:
        return "Article not found", 404
    
    # Convert to dict and process
    article_dict = dict(article)
    
    # Extract category from breadcrumbs
    article_dict['category'] = extract_category_from_breadcrumbs(article_dict.get('breadcrumbs'))
    
    # Parse intended users
    intended_users = article_dict.get('intended_users', '')
    if intended_users:
        article_dict['intended_users_list'] = [u.strip() for u in intended_users.split(',') if u.strip()]
    else:
        article_dict['intended_users_list'] = []
    
    # Build navigation for sidebar
    navigation = build_navigation()
    
    return render_template(
        'help_detail.html', 
        article=article_dict,
        navigation=navigation
    )

@bp.route('/search')
def search():
    """Search endpoint - redirects to help_list with query"""
    query = request.args.get('q', '')
    return redirect(url_for('help_articles.help_list', q=query))

# ============================================================================
# HELP SITE V2 - Documentation-Style Layout
# ============================================================================

@bp.route('/docs')
def docs_index():
    """Documentation home page (v2 - docs style)"""
    navigation = build_navigation()
    return render_template(
        'help_docs.html',
        navigation=navigation,
        article=None,
        current_path=url_for('help_articles.docs_index')
    )

@bp.route('/docs/<int:article_id>')
def docs_article(article_id):
    """Display single article in documentation layout (v2 - docs style)"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get current article
    cursor.execute("SELECT * FROM help_articles WHERE id = ?", (article_id,))
    article = cursor.fetchone()
    
    if not article:
        return "Article not found", 404
    
    article_dict = dict(article)
    
    # Get previous article
    cursor.execute("SELECT id, article_title FROM help_articles WHERE id < ? ORDER BY id DESC LIMIT 1", (article_id,))
    prev_article = cursor.fetchone()
    prev_article_dict = dict(prev_article) if prev_article else None
    
    # Get next article
    cursor.execute("SELECT id, article_title FROM help_articles WHERE id > ? ORDER BY id ASC LIMIT 1", (article_id,))
    next_article = cursor.fetchone()
    next_article_dict = dict(next_article) if next_article else None
    
    # Build navigation
    navigation = build_navigation()
    current_path = url_for('help_articles.docs_article', article_id=article_id)
    
    return render_template(
        'help_docs.html',
        navigation=navigation,
        article=article_dict,
        prev_article=prev_article_dict,
        next_article=next_article_dict,
        current_path=current_path
    )

@bp.route('/api/search')
def api_search():
    """API endpoint for search functionality (v2)"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return jsonify([])
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Use FTS for search
    cursor.execute("""
        SELECT help_articles.id, help_articles.article_title, help_articles.breadcrumbs
        FROM help_articles
        INNER JOIN help_articles_fts ON help_articles.id = help_articles_fts.rowid
        WHERE help_articles_fts MATCH ?
        LIMIT 10
    """, (query,))
    
    results = cursor.fetchall()
    
    # Convert to JSON-serializable format
    results_list = [dict(row) for row in results]
    
    return jsonify(results_list)

@bp.app_template_filter('article_url')
def article_url_filter(article):
    """
    Template filter to generate article URL from article object.
    Usage: {{ article|article_url }}
    """
    slug = generate_article_slug(article)
    return url_for('help_articles.help_detail', article_slug=slug)
