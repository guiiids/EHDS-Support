"""
TeamSupport Archive Viewer - Flask Application
Read-only web interface for historical support tickets.

Uses SQLite database for fast queries and lazy loading.
Run migration first: python migrate_to_sqlite.py
"""

import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, g, redirect, render_template, request, send_file, url_for
from markupsafe import Markup

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Register Blueprints
from blueprints.canned_responses import bp as canned_responses_bp
app.register_blueprint(canned_responses_bp)

# === Template Configuration ===
VALID_TEMPLATES = ['ticket_detail', 'ticket_detail2', 'ticket_detail3', 'ticket_detail4']

def get_selected_template():
    """Get current template selection from .env file."""
    template = os.getenv('TICKET_DETAIL_TEMPLATE', 'ticket_detail')
    return template if template in VALID_TEMPLATES else 'ticket_detail'

# === Database Configuration ===
DB_PATH = os.getenv("DATABASE_PATH")
if DB_PATH:
    DB_PATH = Path(DB_PATH)
else:
    DB_PATH = Path(__file__).resolve().parent.parent / "data" / "teamsupport.db"


def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        db_path = Path(DB_PATH)
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database {DB_PATH} not found! Run: python migrate_to_sqlite.py"
            )
        # Connect in read-only mode to support Azure Blob Storage mounts
        g.db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        g.db.row_factory = sqlite3.Row  # Return dicts instead of tuples
        
        # Attach KB database for unified queries
        kb_path = Path(DB_PATH).parent / "kb_articles.db"
        if kb_path.exists():
            try:
                g.db.execute(f"ATTACH DATABASE '{kb_path}' AS kb")
            except sqlite3.OperationalError:
                pass
    return g.db


@app.teardown_appcontext
def close_db(exception):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


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

def get_filtered_query_parts(search_query: str = None, filters: dict = None):
    """
    Helper to generate SQL WHERE clauses and parameters for unified query.
    Returns: (ticket_where, ticket_params, kb_where, kb_params)
    """
    ticket_conditions = ["1=1"]
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
        # Agent / Author
        if filters.get('agent'):
            ticket_conditions.append("assigned_to = ?")
            ticket_params.append(filters['agent'])
            kb_conditions.append("author = ?")
            kb_params.append(filters['agent'])
            
        # Status
        if filters.get('status'):
            # KB status is always 'Canned Response'
            if filters['status'] == 'Canned Response':
                ticket_conditions.append("1=0") # No tickets match this status
                kb_conditions.append("1=1")
            else:
                ticket_conditions.append("status = ?")
                ticket_params.append(filters['status'])
                kb_conditions.append("1=0") # No KB matches other statuses

        # Category
        if filters.get('category'):
            ticket_conditions.append("ticket_type = ?") # Category maps to ticket_type
            ticket_params.append(filters['category'])
            kb_conditions.append("kb_parent_category_name = ?")
            kb_params.append(filters['category'])

        # Subcategory
        if filters.get('subcategory'):
            ticket_conditions.append("subcategory = ?")
            ticket_params.append(filters['subcategory'])
            kb_conditions.append("kb_category_name = ?")
            kb_params.append(filters['subcategory'])

        # Customer
        if filters.get('customer'):
            ticket_conditions.append("customers = ?")
            ticket_params.append(filters['customer'])
            kb_conditions.append("1=0") # KB has no customers

        # Year
        if filters.get('year'):
            ticket_conditions.append("strftime('%Y', date_action_created) = ?")
            ticket_params.append(filters['year'])
            kb_conditions.append("strftime('%Y', COALESCE(date_modified, date_created)) = ?")
            kb_params.append(filters['year'])

        # Month
        if filters.get('month'):
            ticket_conditions.append("strftime('%m', date_action_created) = ?")
            ticket_params.append(f"{int(filters['month']):02d}") # Ensure 01-12 format
            kb_conditions.append("strftime('%m', COALESCE(date_modified, date_created)) = ?")
            kb_params.append(f"{int(filters['month']):02d}")

    return (
        " AND ".join(ticket_conditions), ticket_params,
        " AND ".join(kb_conditions), kb_params
    )


def get_facets(search_query: str = None, filters: dict = None) -> dict:
    """Calculate facet counts based on current search and filters."""
    db = get_db()
    cursor = db.cursor()
    
    t_where, t_params, k_where, k_params = get_filtered_query_parts(search_query, filters)
    
    # Define the unified dataset CTE
    # We select only columns needed for aggregation to optimize
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
    """
    
    full_params = t_params + k_params
    facets = {}
    
    # Helper to run aggregation
    def get_group_counts(field):
        sql = f"{cte_sql} SELECT {field}, COUNT(*) as c FROM unified_items WHERE {field} IS NOT NULL AND {field} != '' GROUP BY {field} ORDER BY c DESC LIMIT 50"
        try:
            cursor.execute(sql, full_params)
            return [(row[0], row[1]) for row in cursor.fetchall()]
        except sqlite3.OperationalError:
            return []

    # 1. Agent
    facets['agent'] = get_group_counts('agent')
    
    # 2. Status
    facets['status'] = get_group_counts('status')
    
    # 3. Category
    facets['category'] = get_group_counts('category')
    
    # 4. Subcategory
    facets['subcategory'] = get_group_counts('subcategory')
    
    # 5. Customer (Limit to top 20 to avoid huge lists)
    sql_cust = f"{cte_sql} SELECT customer, COUNT(*) as c FROM unified_items WHERE customer IS NOT NULL AND customer != '' GROUP BY customer ORDER BY c DESC LIMIT 20"
    try:
        cursor.execute(sql_cust, full_params)
        facets['customer'] = [(row[0], row[1]) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        facets['customer'] = []

    # 6. Year
    sql_year = f"{cte_sql} SELECT strftime('%Y', date_val) as y, COUNT(*) as c FROM unified_items WHERE date_val IS NOT NULL GROUP BY y ORDER BY y DESC"
    try:
        cursor.execute(sql_year, full_params)
        facets['year'] = [(row[0], row[1]) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
         facets['year'] = []

    # 7. Month (if year selected? or always? let's show always for now or only if year selected usually)
    # Let's show aggregated months across all years for now, or maybe just simple list
    sql_month = f"{cte_sql} SELECT strftime('%m', date_val) as m, COUNT(*) as c FROM unified_items WHERE date_val IS NOT NULL GROUP BY m ORDER BY m ASC"
    try:
        cursor.execute(sql_month, full_params)
        facets['month'] = []
        import calendar
        for row in cursor.fetchall():
            m_idx = int(row[0]) if row[0] and row[0].isdigit() else 0
            if 1 <= m_idx <= 12:
                name = calendar.month_name[m_idx]
                facets['month'].append((m_idx, name, row[1])) # (id, name, count)
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
            assigned_to,
            customers,
            ticket_owner,
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
            author as assigned_to,
            '' as customers,
            'KB' as ticket_owner,
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
             print(f"Warning: KB query failed: {e}")
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
            'ticket_type': 'Canned Response' if row['is_kb'] else 'Ticket',
            'ticket_source': 'Knowledge Base' if row['is_kb'] else 'Email',
            'date_ticket_created': row['date_action_created'],
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
        'ticket_name': row['ticket_name'],
        'status': row['status'],
        'subcategory': row['subcategory'],
        'date_action_created': row['date_action_created'],
        'date_ticket_created': row['date_ticket_created'],
        'date_closed': row['date_closed'],
        'ticket_type': row['ticket_type'],
        'customers': row['customers'],
        'assigned_to': row['assigned_to'],
        'ticket_source': row['ticket_source'],
        'ticket_owner': row['ticket_owner'],
    }


def get_ticket_messages(ticket_id: int) -> list[dict]:
    """Lazy load messages for a specific ticket."""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT * FROM messages 
        WHERE ticket_number = ? 
        ORDER BY date_action_created ASC
    """, (ticket_id,))
    
    rows = cursor.fetchall()
    
    messages = []
    for row in rows:
        messages.append({
            'ticket_number': row['ticket_number'],
            'action_creator_name': row['action_creator_name'],
            'action_type': row['action_type'],
            'date_action_created': row['date_action_created'],
            'action_description': row['action_description'],
            'cleaned_description': row['cleaned_description'],
            'role': row['role'],
        })
    
    return messages


# === Routes ===

@app.route('/')
def index():
    """Admin Hub (Home Page)."""
    return render_template('admin_hub.html')


@app.route('/tickets')
def ticket_list():
    """Ticket list view with search, pagination, and facets."""
    search_query = request.args.get('q', '').strip()
    
    # Parse filters
    filters = {
        'agent': request.args.get('agent', '').strip(),
        'status': request.args.get('status', '').strip(),
        'category': request.args.get('category', '').strip(),
        'subcategory': request.args.get('subcategory', '').strip(),
        'customer': request.args.get('customer', '').strip(),
        'year': request.args.get('year', '').strip(),
        'month': request.args.get('month', '').strip(),
    }
    # Remove empty filters
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
    facets = get_facets(search_query if search_query else None, filters)
    
    # Calculate display range
    start_idx = (page - 1) * per_page
    showing_start = start_idx + 1 if filtered_count > 0 else 0
    showing_end = min(start_idx + per_page, filtered_count)
    
    return render_template(
        'ticket_list.html',
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
    
    # Get ticket info
    ticket_info = get_ticket_info(ticket_id)
    if not ticket_info:
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
    from generate_pdf import generate_ticket_pdf
    
    # Get ticket info
    ticket_info = get_ticket_info(ticket_id)
    if not ticket_info:
        return "Ticket not found", 404
    
    # Get messages for this ticket
    messages = get_ticket_messages(ticket_id)
    
    # Generate PDF in memory
    pdf_buffer = generate_ticket_pdf(ticket_info, messages)
    
    return send_file(
        pdf_buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'ticket_{ticket_id}_summary.pdf'
    )


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
        print(f"❌ Database {DB_PATH} not found!")
        print("   Run migration first: python migrate_to_sqlite.py")
        exit(1)
    
    print(f"✅ Using database: {DB_PATH}")
    app.run(debug=True, port=5525)
