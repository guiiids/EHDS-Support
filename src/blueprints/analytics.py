from flask import Blueprint, render_template, jsonify, request, g
import sqlite3
from datetime import datetime
from ..logger import log_info, log_error, log_warning
from ..db import get_db

bp = Blueprint('analytics', __name__, url_prefix='/analytics')

# Global filters to exclude unmapped/internal data
GLOBAL_FILTER_SQL = """
    LOWER(customers) NOT LIKE '%unknown company%' 
    AND customers != 'Agilent Technologies (688244)'
    AND customers IS NOT NULL 
    AND customers != ''
"""

def get_date_filter(range_str):
    """Return SQL for date filtering based on range string."""
    if not range_str or range_str == 'all':
        return "1=1"
    
    ranges = {
        '7d': "date_ticket_created >= date('now', '-7 days')",
        '30d': "date_ticket_created >= date('now', '-30 days')",
        '90d': "date_ticket_created >= date('now', '-90 days')",
        '12m': "date_ticket_created >= date('now', '-1 year')",
        '2y': "date_ticket_created >= date('now', '-2 years')",
        '5y': "date_ticket_created >= date('now', '-5 years')"
    }
    return ranges.get(range_str, "1=1")

@bp.route('/dashboard')
def dashboard():
    """Render the analytics dashboard page."""
    return render_template('analytics/dashboard.html')

@bp.route('/api/summary')
def api_summary():
    """High-level KPI summary for the dashboard."""
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        
        # 1. Total Unique Customers (Filtered by date)
        cursor.execute(f"SELECT COUNT(DISTINCT customers) FROM tickets WHERE {GLOBAL_FILTER_SQL} AND {date_filter}")
        total_customers = cursor.fetchone()[0]
        
        # 2. Active Customers (Last 30 Days - Fixed period, but respect global filters)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT customers) 
            FROM tickets 
            WHERE {GLOBAL_FILTER_SQL} 
            AND date_ticket_created >= date('now', '-30 days')
        """)
        active_customers = cursor.fetchone()[0]
        
        # 3. Overall Avg Response Time (Respect range)
        cursor.execute(f"""
            SELECT AVG(
                CAST((julianday(m.date_action_created) - julianday(t.date_ticket_created)) * 24 AS REAL)
            )
            FROM tickets t
            JOIN messages m ON t.ticket_number = m.ticket_number
            WHERE {GLOBAL_FILTER_SQL.replace('customers', 't.customers')} AND {date_filter.replace('date_ticket_created', 't.date_ticket_created')}
            AND m.role = 'Agent' AND m.action_type != 'Description'
        """)
        avg_response = cursor.fetchone()[0] or 0
        
        # 4. Overall Avg Resolution Time (Respect range)
        cursor.execute(f"""
            SELECT AVG(
                CAST((julianday(date_closed) - julianday(date_ticket_created)) * 24 AS REAL)
            )
            FROM tickets
            WHERE date_closed IS NOT NULL AND status IN ('Closed', 'Resolved')
            AND {GLOBAL_FILTER_SQL} AND {date_filter}
        """)
        avg_resolution = cursor.fetchone()[0] or 0

        # 5. Total Tickets (Respect range)
        cursor.execute(f"SELECT COUNT(*) FROM tickets WHERE {GLOBAL_FILTER_SQL} AND {date_filter}")
        total_tickets = cursor.fetchone()[0]

        return jsonify({
            'total_customers': total_customers,
            'active_customers': active_customers,
            'avg_response_hours': round(float(avg_response), 1),
            'avg_resolution_hours': round(float(avg_resolution), 1),
            'total_tickets': total_tickets
        })
    except Exception as e:
        log_error(f"Error fetching analytics summary: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/tickets-by-customer')
def tickets_by_customer():
    db = get_db()
    limit = request.args.get('limit', 20, type=int)
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        cursor.execute(f"""
            SELECT customers, COUNT(*) as ticket_count
            FROM tickets
            WHERE {GLOBAL_FILTER_SQL} AND {date_filter}
            GROUP BY customers
            ORDER BY ticket_count DESC
            LIMIT ?
        """, (limit,))
        data = [{'customer': row[0], 'count': row[1]} for row in cursor.fetchall()]
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching tickets by customer: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/customer-activity')
def customer_activity():
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        # Top 5 customers for the timeline (respect range)
        cursor.execute(f"""
            SELECT customers FROM tickets 
            WHERE {GLOBAL_FILTER_SQL} AND {date_filter}
            GROUP BY customers ORDER BY COUNT(*) DESC LIMIT 5
        """)
        top_customers = [row[0] for row in cursor.fetchall()]
        
        if not top_customers:
            return jsonify([])

        placeholders = ', '.join(['?'] * len(top_customers))
        params = top_customers.copy()
        
        cursor.execute(f"""
            SELECT customers, strftime('%Y-%m', date_ticket_created) as month, COUNT(*) as count
            FROM tickets
            WHERE customers IN ({placeholders}) AND {date_filter}
            GROUP BY customers, month
            ORDER BY month ASC
        """, params)
        
        results = cursor.fetchall()
        # Format for ApexCharts (series per customer)
        processed = {}
        for cust in top_customers:
            processed[cust] = {'name': cust, 'data': []}
            
        months = sorted(list(set(row[1] for row in results)))
        for month in months:
            for cust in top_customers:
                # Find count for this customer and month
                count = next((row[2] for row in results if row[0] == cust and row[1] == month), 0)
                processed[cust]['data'].append({'x': month, 'y': count})
                
        return jsonify(list(processed.values()))
    except Exception as e:
        log_error(f"Error fetching customer activity: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/performance-by-customer')
def performance_by_customer():
    db = get_db()
    limit = request.args.get('limit', 15, type=int)
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        cursor.execute(f"""
            SELECT 
                customers,
                AVG(CAST((julianday(date_closed) - julianday(date_ticket_created)) * 24 AS REAL)) as avg_res
            FROM tickets
            WHERE {GLOBAL_FILTER_SQL} AND {date_filter} AND date_closed IS NOT NULL
            GROUP BY customers
            ORDER BY avg_res DESC
            LIMIT ?
        """, (limit,))
        resolution_data = [{'customer': row[0], 'value': round(row[1], 1)} for row in cursor.fetchall()]

        cursor.execute(f"""
            SELECT 
                t.customers,
                AVG(CAST((julianday(m.date_action_created) - julianday(t.date_ticket_created)) * 24 AS REAL)) as avg_resp
            FROM tickets t
            JOIN messages m ON t.ticket_number = m.ticket_number
            WHERE {GLOBAL_FILTER_SQL.replace('customers', 't.customers')}
                AND {date_filter.replace('date_ticket_created', 't.date_ticket_created')}
                AND m.role = 'Agent' AND m.action_type != 'Description'
            GROUP BY t.customers
            ORDER BY avg_resp DESC
            LIMIT ?
        """, (limit,))
        response_data = [{'customer': row[0], 'value': round(row[1], 1)} for row in cursor.fetchall()]

        return jsonify({
            'resolution': resolution_data,
            'response': response_data
        })
    except Exception as e:
        log_error(f"Error fetching performance data: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/reopened-by-customer')
def reopened_by_customer():
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        cursor.execute(f"""
            SELECT 
                customers,
                COUNT(DISTINCT ticket_number) as total,
                SUM(CASE WHEN LOWER(status) = 'reopened' THEN 1 ELSE 0 END) as reopened
            FROM tickets
            WHERE {GLOBAL_FILTER_SQL} AND {date_filter}
            GROUP BY customers
            HAVING reopened > 0
            ORDER BY reopened DESC
            LIMIT 15
        """)
        data = []
        for row in cursor.fetchall():
            percentage = (row[2] / row[1]) * 100 if row[1] > 0 else 0
            data.append({
                'customer': row[0],
                'count': row[2],
                'percentage': round(percentage, 1)
            })
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching reopened tickets: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/churn-at-risk')
def churn_at_risk():
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        cursor.execute(f"""
            SELECT 
                customers,
                MAX(date_ticket_created) as last_ticket,
                CAST((julianday('now') - julianday(MAX(date_ticket_created))) AS INTEGER) as days_idle,
                COUNT(*) as total_history
            FROM tickets
            WHERE {GLOBAL_FILTER_SQL} AND {date_filter}
            GROUP BY customers
            HAVING days_idle > 90
            ORDER BY days_idle DESC
            LIMIT 50
        """)
        data = [{
            'customer': row[0],
            'last_ticket': row[1],
            'days_idle': row[2],
            'total_tickets': row[3]
        } for row in cursor.fetchall()]
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching churn data: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/category-breakdown')
def category_breakdown():
    db = get_db()
    customer = request.args.get('customer', '').strip()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        query = f"""
            SELECT ticket_type, COUNT(*) as count
            FROM tickets
            WHERE ticket_type IS NOT NULL AND ticket_type != ''
            AND {GLOBAL_FILTER_SQL} AND {date_filter}
        """
        params = []
        if customer:
            query += " AND customers = ?"
            params.append(customer)
        
        query += " GROUP BY ticket_type ORDER BY count DESC LIMIT 10"
        cursor.execute(query, params)
        data = [{'category': row[0], 'count': row[1]} for row in cursor.fetchall()]
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching category breakdown: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/source-distribution')
def source_distribution():
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        cursor.execute(f"""
            SELECT ticket_source, COUNT(*) as count
            FROM tickets
            WHERE ticket_source IS NOT NULL AND ticket_source != ''
            AND {GLOBAL_FILTER_SQL} AND {date_filter}
            GROUP BY ticket_source
            ORDER BY count DESC
        """)
        data = [{'source': row[0], 'count': row[1]} for row in cursor.fetchall()]
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching source distribution: {e}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/loyalty-metrics')
def loyalty_metrics():
    db = get_db()
    date_filter = get_date_filter(request.args.get('range'))
    try:
        cursor = db.cursor()
        # Segment customers by ticket count (respect range)
        cursor.execute(f"""
            WITH customer_stats AS (
                SELECT customers, COUNT(*) as total_tickets
                FROM tickets
                WHERE {GLOBAL_FILTER_SQL} AND {date_filter}
                GROUP BY customers
            )
            SELECT 
                CASE 
                    WHEN total_tickets = 1 THEN 'New (1 Ticket)'
                    WHEN total_tickets BETWEEN 2 AND 5 THEN 'Returning (2-5)'
                    ELSE 'Loyal (5+)'
                END as segment,
                COUNT(*) as customer_count
            FROM customer_stats
            GROUP BY segment
        """)
        data = [{'segment': row[0], 'count': row[1]} for row in cursor.fetchall()]
        return jsonify(data)
    except Exception as e:
        log_error(f"Error fetching loyalty metrics: {e}")
        return jsonify({'error': str(e)}), 500
