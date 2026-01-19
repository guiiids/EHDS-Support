"""
TeamSupport Archive Viewer - Flask Application
Read-only web interface for historical support tickets.

Uses SQLite database for fast queries and lazy loading.
Run migration first: python migrate_to_sqlite.py
"""

import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, g, redirect, render_template, request, send_file, url_for
from markupsafe import Markup

from .db import get_db, close_db

# Load environment variables
load_dotenv()

app = Flask(__name__)

# === Logging Setup ===
from .logger import setup_logging, RequestLogger, get_logger, log_info, log_error, log_warning

# Initialize logging (creates logs/ directory and configures handlers)
logger = setup_logging(app)

# Initialize request logging middleware (tracks all HTTP requests)
request_logger = RequestLogger(app)

# Register Blueprints
from .blueprints.canned_responses import bp as canned_responses_bp
from .blueprints.help_articles import bp as help_articles_bp
from .blueprints.chat_widget import bp as chat_widget_bp
from .blueprints.analytics import bp as analytics_bp

app.register_blueprint(canned_responses_bp)
app.register_blueprint(help_articles_bp)
app.register_blueprint(chat_widget_bp)
app.register_blueprint(analytics_bp)
logger.info("Blueprints registered: canned_responses, help_articles, chat_widget, analytics")

# === Template Configuration ===
VALID_TEMPLATES = ['ticket_detail', 'ticket_detail2', 'ticket_detail3', 'ticket_detail4']

def get_selected_template():
    """Get current template selection from .env file."""
    template = os.getenv('TICKET_DETAIL_TEMPLATE', 'ticket_detail')
    return template if template in VALID_TEMPLATES else 'ticket_detail'

@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)


# === Helper Functions ===

def extract_signature(text: str) -> tuple[str, str]:
    """
    Detect and extract email signature from message body.
    Returns: (main_body, signature)
    """
    if not text:
        return text, ""
    
    agent_names = [
        'Nadia Clark', 'Nadia',
        'Vinod Rajendran', 'Vinod',
        'Sook-Theng Chow', 'Sook',
        'William Lai', 'William',
        'Elvira Carrera', 'Elvira',
        'Sophie Katsarova', 'Sophie',
        'Guilherme Vieira Machado', 'Guilherme Vieira-Machado', 'Guilherme',
    ]
    
    names_pattern = '|'.join(re.escape(name) for name in agent_names)
    
    greeting_pattern = rf'''
        (.*?)
        (
            (?:Thanks|Thank\s*you|Regards|Best|Best\s*regards|Cheers|Sincerely|Warm\s*regards|Kind\s*regards|Many\s*thanks)
            [,!]?\s*\n
            (?:{names_pattern})
            .*
        )
        $
    '''
    
    match = re.search(greeting_pattern, text, flags=re.IGNORECASE | re.DOTALL | re.VERBOSE)
    
    if match:
        return match.group(1).strip(), match.group(2).strip()
    
    # Nadia-specific pattern
    nadia_pattern = rf'''
        (.*?)
        (
            \n
            Nadia
            \s*\n
            \n?
            Nadia\s+D\.?\s+Clark
            .+
        )
        $
    '''
    
    match = re.search(nadia_pattern, text, flags=re.IGNORECASE | re.DOTALL | re.VERBOSE)
    
    if match:
        return match.group(1).strip(), match.group(2).strip()
    
    return text, ""


def linkify_urls(text: str) -> str:
    """Convert plain URLs to clickable links."""
    if not text:
        return ""
    
    url_pattern = r'(https?://[^\s<>"\')]+)'
    
    def replace_url(match):
        url = match.group(1)
        while url and url[-1] in '.,;:!?)':
            url = url[:-1]
        return f'<a href="{url}" target="_blank" class="text-blue-600 hover:underline">{url}</a>'
    
    return re.sub(url_pattern, replace_url, text)


def format_iso_date(iso_str: str, format_str: str = '%m/%d/%y %I:%M %p') -> str:
    """Convert ISO date string to display format."""
    if not iso_str or iso_str == 'None':
        return 'N/A'
    try:
        if 'T' in iso_str:
            dt = datetime.fromisoformat(iso_str)
        else:
            try:
                dt = datetime.strptime(iso_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dt = datetime.strptime(iso_str, '%Y-%m-%d')
        return dt.strftime(format_str)
    except (ValueError, TypeError):
        return str(iso_str)


# === Data Access Layer ===

def get_filtered_query_parts(search_query: str = None, filters: dict = None, exclude_field: str = None):
    """
    Helper to generate SQL WHERE clauses and parameters for unified query.
    Returns: (ticket_where, ticket_params, kb_where, kb_params)
    """
    ticket_conditions = [
        "1=1",
        "LOWER(customers) NOT LIKE '%unknown company%'",
        "customers != 'Agilent Technologies (688244)'",
        "customers IS NOT NULL",
        "customers != ''"
    ]
    ticket_params = []
    
    kb_conditions = ["1=1"]
    kb_params = []
    
    # 1. Text Search
    if search_query:
        query_param = f"%{search_query}%"
        
        ticket_conditions.append("(CAST(ticket_number AS TEXT) LIKE ? OR LOWER(ticket_name) LIKE LOWER(?))")
        ticket_params.extend([query_param, query_param])
        
        kb_conditions.append("(LOWER(title) LIKE LOWER(?) OR CAST(ticket_number AS TEXT) LIKE ?)")
        kb_params.extend([query_param, query_param])

    # 2. Facet Filters
    if filters:
        
        def add_condition(field, db_col, ticket_list=None, kb_list=None, is_ticket=True, is_kb=True):
            if filters.get(field) and exclude_field != field:
                values = filters[field]
                if not isinstance(values, list):
                    values = [values]
                
                placeholders = ','.join(['?'] * len(values))
                
                if is_ticket:
                    ticket_list.append(f"{db_col} IN ({placeholders})")
                    ticket_params.extend(values)
                
                if is_kb:
                    # Special Case: KB Status is always 'Canned Response'
                    if field == 'status':
                        if 'Canned Response' in values:
                             kb_list.append("1=1")
                        else:
                             kb_list.append("1=0")
                    else:
                        kb_list.append(f"{db_col if db_col != 'ticket_type' else 'kb_parent_category_name'} IN ({placeholders})")
                        if field != 'status': # Status is handled above
                             kb_params.extend(values)

        # Agent / Author
        if filters.get('agent') and exclude_field != 'agent':
             vals = filters['agent']
             ph = ','.join(['?'] * len(vals))
             ticket_conditions.append(f"assigned_to IN ({ph})")
             ticket_params.extend(vals)
             kb_conditions.append(f"author IN ({ph})")
             kb_params.extend(vals)

        # Status
        if filters.get('status') and exclude_field != 'status':
            vals = filters['status']
            ph = ','.join(['?'] * len(vals))
            ticket_conditions.append(f"status IN ({ph})")
            ticket_params.extend(vals)
            
            if 'Canned Response' in vals:
                kb_conditions.append("1=1")
            else:
                 kb_conditions.append("1=0")

        # Category (Type)
        if filters.get('category') and exclude_field != 'category':
             vals = filters['category']
             ph = ','.join(['?'] * len(vals))
             ticket_conditions.append(f"ticket_type IN ({ph})")
             ticket_params.extend(vals)
             kb_conditions.append(f"kb_parent_category_name IN ({ph})")
             kb_params.extend(vals)

        # Subcategory
        if filters.get('subcategory') and exclude_field != 'subcategory':
             vals = filters['subcategory']
             ph = ','.join(['?'] * len(vals))
             ticket_conditions.append(f"subcategory IN ({ph})")
             ticket_params.extend(vals)
             kb_conditions.append(f"kb_category_name IN ({ph})")
             kb_params.extend(vals)

        # Customer
        if filters.get('customer') and exclude_field != 'customer':
             vals = filters['customer']
             ph = ','.join(['?'] * len(vals))
             ticket_conditions.append(f"customers IN ({ph})")
             ticket_params.extend(vals)
             kb_conditions.append("1=0")

        # Date Logic Helper
        def add_date_logic(filter_key, db_col_ticket, db_col_kb):
             if filters.get(filter_key):
                 val = filters[filter_key]
                 # Handle list if it came as a list (though date usually single, but for safety)
                 if isinstance(val, list): val = val[0]
                 
                 now = datetime.now()
                 start_date = None
                 end_date = None
                 
                 if val == 'today':
                     start_date = now.strftime('%Y-%m-%d')
                 elif val == 'last_7_days':
                     start_date = (now - timedelta(days=7)).strftime('%Y-%m-%d')
                 elif val == 'last_30_days':
                     start_date = (now - timedelta(days=30)).strftime('%Y-%m-%d')
                 elif val == 'this_week': # For last_modified
                      start_of_week = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
                      start_date = start_of_week
                 elif val == 'this_month':
                      start_of_month = now.replace(day=1).strftime('%Y-%m-%d')
                      start_date = start_of_month
                 elif val == 'custom':
                      # Look for start/end in filters
                      start_key = f"{filter_key}_start"
                      end_key = f"{filter_key}_end"
                      if filters.get(start_key):
                          start_date = filters[start_key]
                      if filters.get(end_key):
                          end_date = filters[end_key]
                 
                 if start_date:
                     ticket_conditions.append(f"{db_col_ticket} >= ?")
                     ticket_params.append(start_date)
                     kb_conditions.append(f"COALESCE({db_col_kb}, '1970-01-01') >= ?")
                     kb_params.append(start_date)
                 
                 if end_date:
                     # Add time for end date to make it inclusive of the day (23:59:59) or just check < next day
                     # For simplicity, assuming simple date string comparison
                     ticket_conditions.append(f"date({db_col_ticket}) <= ?")
                     ticket_params.append(end_date)
                     kb_conditions.append(f"date(COALESCE({db_col_kb}, '1970-01-01')) <= ?")
                     kb_params.append(end_date)

        # Date Created
        add_date_logic('date_created', 'date_ticket_created', 'date_created')
        
        # Last Modified
        add_date_logic('last_modified', 'date_action_created', 'date_modified')

        # Legacy Year/Month (keep for compatibility if needed, using simple equality)
        if filters.get('year') and exclude_field != 'year':
            ticket_conditions.append("strftime('%Y', date_action_created) = ?")
            ticket_params.append(filters['year'])
            kb_conditions.append("strftime('%Y', COALESCE(date_modified, date_created)) = ?")
            kb_params.append(filters['year'])

        if filters.get('month') and exclude_field != 'month':
            m = int(filters['month'])
            ticket_conditions.append("strftime('%m', date_action_created) = ?")
            ticket_params.append(f"{m:02d}")
            kb_conditions.append("strftime('%m', COALESCE(date_modified, date_created)) = ?")
            kb_params.append(f"{m:02d}")

    return (
        " AND ".join(ticket_conditions), ticket_params,
        " AND ".join(kb_conditions), kb_params
    )
from datetime import timedelta # Ensure timedelta is available

def get_facets(search_query: str = None, filters: dict = None) -> dict:
    """Calculate facet counts with exclusion logic for multi-select friendliness."""
    db = get_db()
    cursor = db.cursor()
    
    facets = {}

    # Define helper to get counts for a specific field, excluding its own filter
    def fetch_facet(field, facet_key):
        # recalculate WHERE clause excluding current field
        t_where, t_params, k_where, k_params = get_filtered_query_parts(search_query, filters, exclude_field=facet_key)
        
        full_params = t_params + k_params
        
        # CTE for this specific facet query
        # Note: We can reuse the same CTE structure but the WHERE clause changes per facet
        cte_sql = f"""
            WITH unified_items AS (
                SELECT 
                    assigned_to as agent,
                    status,
                    ticket_type as category,
                    subcategory,
                    customers as customer,
                    date_action_created as date_val,
                    0 as is_kb
                FROM tickets
                WHERE {t_where}
                
                UNION ALL
                
                SELECT 
                    author as agent,
                    'Canned Response' as status,
                    kb_parent_category_name as category,
                    kb_category_name as subcategory,
                    '' as customer,
                    COALESCE(date_modified, date_created) as date_val,
                    1 as is_kb
                FROM kb.kb_articles
                WHERE {k_where}
            )
            SELECT {field}, COUNT(*) as c 
            FROM unified_items 
            WHERE {field} IS NOT NULL AND {field} != '' 
            GROUP BY {field} 
            ORDER BY c DESC 
            LIMIT 50
        """
        
        try:
            cursor.execute(cte_sql, full_params)
            return [(row[0], row[1]) for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            log_warning(f"Facet error for {field}: {e}")
            return []

    # 1. Agent (exclude 'agent' filter)
    facets['agent'] = fetch_facet('agent', 'agent')
    
    # 2. Status (exclude 'status' filter)
    facets['status'] = fetch_facet('status', 'status')
    
    # 3. Category (exclude 'category' filter)
    facets['category'] = fetch_facet('category', 'category')
    
    # 4. Subcategory
    facets['subcategory'] = fetch_facet('subcategory', 'subcategory')
    
    # 5. Customer
    # Customer logic slightly different in original (limit 20), but unified generic approach is fine or special case
    # Let's use the generic fetch_facet but logic on limit is inside query.
    # We can just use fetch_facet('customer', 'customer') as it limits to 50
    facets['customer'] = fetch_facet('customer', 'customer')

    # 6. Year
    # For Year/Month, usually we don't exclude unless we want multi-year Select
    # Let's exclude for consistency
    t_where_y, t_params_y, k_where_y, k_params_y = get_filtered_query_parts(search_query, filters, exclude_field='year')
    sql_year = f"""
        WITH unified_items AS (
            SELECT date_action_created as date_val FROM tickets WHERE {t_where_y}
            UNION ALL
            SELECT COALESCE(date_modified, date_created) as date_val FROM kb.kb_articles WHERE {k_where_y}
        )
        SELECT strftime('%Y', date_val) as y, COUNT(*) as c 
        FROM unified_items 
        WHERE date_val IS NOT NULL 
        GROUP BY y 
        ORDER BY y DESC
    """
    try:
        cursor.execute(sql_year, t_params_y + k_params_y)
        facets['year'] = [(row[0], row[1]) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        facets['year'] = []

    # 7. Month (Aggregated)
    # Reuse year params for simplicity or own exclusion
    t_where_m, t_params_m, k_where_m, k_params_m = get_filtered_query_parts(search_query, filters, exclude_field='month')
    sql_month = f"""
        WITH unified_items AS (
            SELECT date_action_created as date_val FROM tickets WHERE {t_where_m}
            UNION ALL
            SELECT COALESCE(date_modified, date_created) as date_val FROM kb.kb_articles WHERE {k_where_m}
        )
        SELECT strftime('%m', date_val) as m, COUNT(*) as c 
        FROM unified_items 
        WHERE date_val IS NOT NULL 
        GROUP BY m 
        ORDER BY m ASC
    """
    try:
        cursor.execute(sql_month, t_params_m + k_params_m)
        facets['month'] = []
        import calendar
        for row in cursor.fetchall():
             m_idx = int(row[0]) if row[0] and row[0].isdigit() else 0
             if 1 <= m_idx <= 12:
                 name = calendar.month_name[m_idx]
                 facets['month'].append((m_idx, name, row[1]))
    except sqlite3.OperationalError:
         facets['month'] = []
         
    return facets


def get_ticket_count(search_query: str = None, filters: dict = None) -> tuple[int, int]:
    """Get total and filtered ticket counts (including KB Articles)."""
    db = get_db()
    cursor = db.cursor()
    
    # Total count (Simple approximation: Tickets + KB total, ignoring filters for 'Total' metric)
    cursor.execute("SELECT COUNT(*) FROM tickets")
    tickets_count = cursor.fetchone()[0]
    
    kb_count = 0
    try:
        cursor.execute("SELECT COUNT(*) FROM kb.kb_articles")
        kb_count = cursor.fetchone()[0]
    except sqlite3.Error:
        pass
        
    total = tickets_count + kb_count
    
    # Filtered count
    t_where, t_params, k_where, k_params = get_filtered_query_parts(search_query, filters)
    
    filtered_tickets = 0
    cursor.execute(f"SELECT COUNT(*) FROM tickets WHERE {t_where}", t_params)
    filtered_tickets = cursor.fetchone()[0]
    
    filtered_kb = 0
    try:
        cursor.execute(f"SELECT COUNT(*) FROM kb.kb_articles WHERE {k_where}", k_params)
        filtered_kb = cursor.fetchone()[0]
    except sqlite3.Error:
        pass
        
    filtered = filtered_tickets + filtered_kb
    
    return total, filtered


def get_tickets_page(page: int, per_page: int, search_query: str = None, filters: dict = None) -> list[dict]:
    """Get paginated unified list of Tickets and KB Articles."""
    db = get_db()
    cursor = db.cursor()
    
    offset = (page - 1) * per_page
    
    t_where, t_params, k_where, k_params = get_filtered_query_parts(search_query, filters)
    
    ticket_select = f"""
        SELECT 
            ticket_number,
            ticket_name,
            status,
            subcategory,
            date_action_created,
            date_ticket_created,
            assigned_to,
            customers,
            ticket_owner,
            ticket_type,
            0 as is_kb,
            ticket_number as real_id,
            ticket_number as display_id
        FROM tickets
        WHERE {t_where}
    """
    
    kb_select = f"""
        SELECT 
            COALESCE(ticket_number, id) as ticket_number,
            title as ticket_name,
            'Canned Response' as status,
            category_name as subcategory,
            COALESCE(date_modified, date_created) as date_action_created,
            date_created as date_ticket_created,
            author as assigned_to,
            '' as customers,
            'KB' as ticket_owner,
            'Canned Response' as ticket_type,
            1 as is_kb,
            id as real_id,
            COALESCE(ticket_number, id) as display_id
        FROM kb.kb_articles
        WHERE {k_where}
    """
    
    full_params = t_params + k_params + [per_page, offset]
    
    sql = f"""
        SELECT * FROM (
            {ticket_select}
            UNION ALL
            {kb_select}
        )
        ORDER BY date_action_created DESC
        LIMIT ? OFFSET ?
    """
    
    try:
        cursor.execute(sql, full_params)
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            # Fallback
             log_warning(f"KB query failed, using fallback: {e}")
             sql = f"""
                SELECT 
                    ticket_number, ticket_name, status, subcategory, date_action_created, assigned_to, customers, ticket_owner, 
                    0 as is_kb, ticket_number as real_id, ticket_number as display_id
                FROM tickets
                WHERE {t_where}
                ORDER BY date_action_created DESC
                LIMIT ? OFFSET ?
            """
             cursor.execute(sql, t_params + [per_page, offset])
        else:
            log_error(f"Database query failed: {e}", exc_info=True)
            raise e
            
    rows = cursor.fetchall()
    
    items = []
    for row in rows:
        items.append({
            'ticket_number': row['display_id'],
            'ticket_name': row['ticket_name'],
            'status': row['status'],
            'subcategory': row['subcategory'],
            'date_action_created': row['date_action_created'],
            'assigned_to': row['assigned_to'],
            'customers': row['customers'],
            'ticket_owner': row['ticket_owner'],
            'is_kb': bool(row['is_kb']),
            'real_id': row['real_id'],
            # Compatibility fields
            'ticket_type': row['ticket_type'] if row['ticket_type'] else ('Canned Response' if row['is_kb'] else 'Ticket'),
            'ticket_source': 'Knowledge Base' if row['is_kb'] else 'Email',
            'date_ticket_created': row['date_ticket_created'],
            'date_closed': None
        })
    
    return items


def get_ticket_info(ticket_id: int) -> dict:
    """Get ticket summary info."""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT * FROM tickets WHERE ticket_number = ?", (ticket_id,))
    row = cursor.fetchone()
    
    if not row:
        return None
    
    return {
        'ticket_number': row['ticket_number'],
        'ticket_name': row['ticket_name'] or 'Untitled Ticket',
        'status': row['status'] or 'Unknown',
        'subcategory': row['subcategory'] or '',
        'date_action_created': row['date_action_created'],
        'date_ticket_created': row['date_ticket_created'],
        'date_closed': row['date_closed'],
        'ticket_type': row['ticket_type'] or 'Ticket',
        'customers': row['customers'] or '',
        'assigned_to': row['assigned_to'] or 'Unassigned',
        'ticket_source': row['ticket_source'] or 'Portal',
        'ticket_owner': row['ticket_owner'] or '',
    }


def get_ticket_messages(ticket_id: int) -> list[dict]:
    """Lazy load messages for a specific ticket."""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT * FROM messages 
        WHERE ticket_number = ? 
        ORDER BY date_action_created DESC
    """, (ticket_id,))
    
    rows = cursor.fetchall()
    
    messages = []
    for row in rows:
        messages.append({
            'ticket_number': row['ticket_number'],
            'action_creator_name': row['action_creator_name'] or '',
            'action_type': row['action_type'] or 'Note',
            'date_action_created': row['date_action_created'],
            'action_description': row['action_description'] or '',
            'cleaned_description': row['cleaned_description'] or '',
            'role': row['role'] or 'Customer',
        })
    
    return messages


# === Routes ===

@app.route('/')
def index():
    """Admin Hub (Home Page)."""
    return render_template('admin_hub.html')


@app.route('/submit-ticket')
def submit_ticket():
    """Submit new ticket form."""
    return render_template('submit_ticket.html')


@app.route('/submit-ticket-auth')
def submit_ticket_auth():
    """Submit new ticket form for logged-in users (streamlined, no contact fields)."""
    # Mock user data - in a real app, this would come from the session/auth system
    current_user = {
        'name': 'Guilherme Vieira Machado',
        'email': 'gvieiramachado@agilent.com',
        'initials': 'GM',
        'organization': 'Agilent Technologies',
        'role': 'Super Admin'
    }
    return render_template('submit_ticket_auth.html', current_user=current_user)


@app.route('/tickets')
def ticket_list():
    """Ticket list view with search, pagination, and facets."""
    search_query = request.args.get('q', '').strip()
    
    # Parse filters
    # Parse filters
    filters = {
        'agent': request.args.getlist('agent'),
        'status': request.args.getlist('status'),
        'type': request.args.getlist('type'),
        # Support both 'type' (frontend) and 'category' (legacy)
        'category': request.args.getlist('category') or request.args.getlist('type'),
        'subcategory': request.args.getlist('subcategory'),
        'customer': request.args.getlist('customer'),
        'date_created': request.args.get('date_created', '').strip(),
        'date_created_start': request.args.get('date_created_start', '').strip(),
        'date_created_end': request.args.get('date_created_end', '').strip(),
        'last_modified': request.args.get('last_modified', '').strip(),
        'last_modified_start': request.args.get('last_modified_start', '').strip(),
        'last_modified_end': request.args.get('last_modified_end', '').strip(),
        'year': request.args.get('year', '').strip(),
        'month': request.args.get('month', '').strip(),
    }
    # Remove empty filters lists or strings (but keep 0 if valid, though here strings)
    filters = {k: v for k, v in filters.items() if v}
    
    # Pagination parameters
    try:
        page = max(1, int(request.args.get('page', 1)))
    except (ValueError, TypeError):
        page = 1
    
    try:
        per_page = int(request.args.get('per_page', 20))
        if per_page not in [10, 20, 25, 50, 100]:
            per_page = 20
    except (ValueError, TypeError):
        per_page = 20
    
    # Get counts
    total_count, filtered_count = get_ticket_count(search_query if search_query else None, filters)
    
    # Calculate pagination
    total_pages = max(1, (filtered_count + per_page - 1) // per_page)
    page = min(page, total_pages)
    
    # Get paginated tickets
    tickets = get_tickets_page(page, per_page, search_query if search_query else None, filters)
    
    # Get Facets (for Sidebar)
    # We pass strict search_query so facets reflect the search
    facets = get_facets(search_query if search_query else None, filters)
    
    # Calculate display range
    start_idx = (page - 1) * per_page
    showing_start = start_idx + 1 if filtered_count > 0 else 0
    showing_end = min(start_idx + per_page, filtered_count)
    
    return render_template(
        'ticket_list2.html',
        tickets=tickets,
        search_query=search_query,
        filters=filters,
        facets=facets,
        total_count=total_count,
        filtered_count=filtered_count,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        showing_start=showing_start,
        showing_end=showing_end,
        current_template=get_selected_template(),
        valid_templates=VALID_TEMPLATES
    )


@app.route('/ticket/<int:ticket_id>')
def ticket_detail(ticket_id: int):
    """Ticket detail view with conversation history (lazy loaded)."""
    log_info(f"Viewing ticket detail", ticket_id=ticket_id)
    
    # Get ticket info
    ticket_info = get_ticket_info(ticket_id)
    if not ticket_info:
        log_warning(f"Ticket not found", ticket_id=ticket_id)
        return "Ticket not found", 404
    
    # Lazy load messages for this ticket only
    messages = get_ticket_messages(ticket_id)
    
    # Prepare messages with linkified content and extracted signatures
    for msg in messages:
        cleaned = msg.get('cleaned_description', '') or ''
        main_body, signature = extract_signature(cleaned)
        msg['linkified_description'] = Markup(linkify_urls(main_body))
        msg['full_message'] = Markup(linkify_urls(cleaned)) if signature else ""
        msg['has_signature'] = bool(signature)
    
    selected_template = get_selected_template()
    return render_template(
        f'{selected_template}.html',
        ticket_id=ticket_id,
        ticket_info=ticket_info,
        messages=messages
    )


@app.route('/set-template', methods=['POST'])
def set_template():
    """Update the template selection in .env file."""
    template = request.form.get('template', 'ticket_detail')
    
    if template not in VALID_TEMPLATES:
        template = 'ticket_detail'
    
    # Read current .env content
    env_path = Path('.env')
    if env_path.exists():
        content = env_path.read_text()
        # Update existing TICKET_DETAIL_TEMPLATE line
        import re as re_module
        if 'TICKET_DETAIL_TEMPLATE=' in content:
            content = re_module.sub(
                r'TICKET_DETAIL_TEMPLATE=.*',
                f'TICKET_DETAIL_TEMPLATE={template}',
                content
            )
        else:
            content += f'\nTICKET_DETAIL_TEMPLATE={template}\n'
    else:
        content = f'TICKET_DETAIL_TEMPLATE={template}\n'
    
    env_path.write_text(content)
    
    # Reload environment
    load_dotenv(override=True)
    
    # Redirect back to ticket list
    return redirect(url_for('ticket_list'))


@app.route('/ticket/<int:ticket_id>/pdf')
def ticket_pdf(ticket_id: int):
    """Generate and download PDF summary for a ticket."""
    from .generate_pdf import generate_ticket_pdf
    
    log_info(f"PDF download requested", ticket_id=ticket_id)
    
    # Get ticket info
    ticket_info = get_ticket_info(ticket_id)
    if not ticket_info:
        log_warning(f"PDF generation failed - ticket not found", ticket_id=ticket_id)
        return "Ticket not found", 404
    
    # Get messages for this ticket
    messages = get_ticket_messages(ticket_id)
    
    try:
        # Generate PDF in memory
        pdf_buffer = generate_ticket_pdf(ticket_info, messages)
        log_info(f"PDF generated successfully", ticket_id=ticket_id, message_count=len(messages))
        
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'ticket_{ticket_id}_summary.pdf'
        )
    except Exception as e:
        log_error(f"PDF generation failed", ticket_id=ticket_id, error=str(e), exc_info=True)
        return "Error generating PDF", 500


# === Template Filters ===

@app.template_filter('format_date')
def format_date(value, format='%m/%d/%y %I:%M %p'):
    """Format datetime for display in US 12-hour format with AM/PM."""
    if value is None:
        return 'N/A'
    if isinstance(value, str):
        return format_iso_date(value, format)
    try:
        return value.strftime(format)
    except:
        return str(value)


@app.template_filter('get_initials')
def get_initials(name):
    """Get first letter of name for avatar."""
    if not name:
        return '?'
    return str(name)[0].upper()


@app.template_filter('status_color')
def status_color(status):
    """Return Tailwind CSS classes based on status."""
    if not status:
        return 'bg-slate-100 text-slate-700 border-slate-200'
    
    status_lower = str(status).lower()
    
    if 'resolved' in status_lower or 'closed' in status_lower:
        return 'bg-emerald-50 text-emerald-700 border-emerald-200'
    elif 'open' in status_lower or 'new' in status_lower:
        return 'bg-amber-50 text-amber-700 border-amber-200'
    elif 'pending' in status_lower:
        return 'bg-blue-50 text-blue-700 border-blue-200'
    else:
        return 'bg-slate-100 text-slate-700 border-slate-200'


if __name__ == '__main__':
    # Check database exists before starting
    if not Path(DB_PATH).exists():
        logger.error(f"Database {DB_PATH} not found!")
        logger.error("Run migration first: python migrate_to_sqlite.py")
        exit(1)
    
    logger.info(f"Starting development server on port 5525")
    logger.info(f"Using database: {DB_PATH}")
    app.run(debug=True, port=5525)
