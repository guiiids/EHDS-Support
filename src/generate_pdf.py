"""
PDF Generator for TeamSupport Ticket Summaries
Generates professional enterprise-grade PDF reports for support tickets.
"""

import io
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    PageBreak, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT


def get_styles():
    """Create custom paragraph styles for the PDF."""
    styles = getSampleStyleSheet()
    
    # Header title style
    styles.add(ParagraphStyle(
        name='ReportTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=6,
        textColor=colors.HexColor('#1f2937'),
        fontName='Helvetica-Bold',
    ))
    
    # Subtitle style
    styles.add(ParagraphStyle(
        name='ReportSubtitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#6b7280'),
        spaceAfter=20,
    ))
    
    # Section header style
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading2'],
        fontSize=14,
        spaceBefore=16,
        spaceAfter=10,
        textColor=colors.HexColor('#374151'),
        fontName='Helvetica-Bold',
        borderPadding=(0, 0, 4, 0),
    ))
    
    # Ticket name style
    styles.add(ParagraphStyle(
        name='TicketName',
        parent=styles['Normal'],
        fontSize=16,
        spaceAfter=12,
        textColor=colors.HexColor('#111827'),
        fontName='Helvetica-Bold',
        leading=20,
    ))
    
    # Label style (for metadata labels)
    styles.add(ParagraphStyle(
        name='Label',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#6b7280'),
        fontName='Helvetica-Bold',
    ))
    
    # Value style (for metadata values)
    styles.add(ParagraphStyle(
        name='Value',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#1f2937'),
        fontName='Helvetica',
    ))
    
    # Message sender style
    styles.add(ParagraphStyle(
        name='MessageSender',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#1f2937'),
        fontName='Helvetica-Bold',
        spaceAfter=2,
    ))
    
    # Message timestamp style
    styles.add(ParagraphStyle(
        name='MessageTime',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#9ca3af'),
        spaceAfter=6,
    ))
    
    # Message body style
    styles.add(ParagraphStyle(
        name='MessageBody',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#374151'),
        leading=14,
        spaceAfter=4,
    ))
    
    # Agent badge style
    styles.add(ParagraphStyle(
        name='AgentBadge',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#4f46e5'),
        fontName='Helvetica-Bold',
    ))
    
    # Customer badge style
    styles.add(ParagraphStyle(
        name='CustomerBadge',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#6b7280'),
        fontName='Helvetica-Bold',
    ))
    
    # Footer style
    styles.add(ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#9ca3af'),
        alignment=TA_CENTER,
    ))
    
    return styles


def format_date(date_str: str) -> str:
    """Format date string for display."""
    if not date_str or date_str == 'None' or date_str == 'N/A':
        return 'N/A'
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%m/%d/%y %I:%M %p')
    except (ValueError, TypeError):
        return str(date_str)


def get_status_color(status: str) -> colors.Color:
    """Return color based on status."""
    if not status:
        return colors.HexColor('#6b7280')
    
    status_lower = str(status).lower()
    
    if 'resolved' in status_lower or 'closed' in status_lower:
        return colors.HexColor('#059669')  # Green
    elif 'open' in status_lower or 'new' in status_lower:
        return colors.HexColor('#d97706')  # Amber
    elif 'pending' in status_lower:
        return colors.HexColor('#2563eb')  # Blue
    else:
        return colors.HexColor('#6b7280')  # Gray


def clean_text_for_pdf(text: str) -> str:
    """Clean text for safe PDF rendering."""
    if not text:
        return ''
    # Replace problematic characters
    text = str(text)
    # Handle XML special characters
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    # Replace tabs and multiple spaces
    text = text.replace('\t', '    ')
    return text


def create_header_section(styles, ticket_info: dict) -> list:
    """Create the header section of the PDF."""
    elements = []
    
    # Report branding
    elements.append(Paragraph("SUPPORT TICKET SUMMARY", styles['ReportSubtitle']))
    
    # Ticket number badge (now second and smaller)
    ticket_num = ticket_info.get('ticket_number', 'Unknown')
    elements.append(Paragraph(f"Ticket #{ticket_num}", styles['TicketName']))
       
    # Ticket name/subject (now first and larger)
    ticket_name = clean_text_for_pdf(ticket_info.get('ticket_name', 'Untitled Ticket'))
    elements.append(Paragraph(ticket_name, styles['ReportTitle']))

    # Generation timestamp
    gen_time = datetime.now().strftime('%B %d, %Y at %I:%M %p')
    elements.append(Paragraph(f"Generated: {gen_time}", styles['MessageTime']))
    
    elements.append(Spacer(1, 20))
    
    return elements


def create_overview_section(styles, ticket_info: dict) -> list:
    """Create the ticket overview section."""
    elements = []
    
    elements.append(Paragraph("📋 TICKET OVERVIEW", styles['SectionHeader']))
    
    # Create overview table
    status = ticket_info.get('status', 'Unknown')
    status_color = get_status_color(status)
    
    data = [
        ['Status:', status or 'Unknown'],
        ['Type:', ticket_info.get('ticket_type', 'N/A') or 'N/A'],
        ['Category:', ticket_info.get('subcategory', 'N/A') or 'N/A'],
        ['Source:', ticket_info.get('ticket_source', 'N/A') or 'N/A'],
    ]
    
    table = Table(data, colWidths=[1.5*inch, 4.5*inch])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#6b7280')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#1f2937')),
        ('TEXTCOLOR', (1, 0), (1, 0), status_color),  # Status color
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
    ]))
    
    elements.append(table)
    elements.append(Spacer(1, 10))
    
    return elements


def create_people_section(styles, ticket_info: dict) -> list:
    """Create the people section."""
    elements = []
    
    elements.append(Paragraph("👥 PEOPLE", styles['SectionHeader']))
    
    data = [
        ['Contact:', ticket_info.get('ticket_owner', 'Unknown') or 'Unknown'],
        ['Assigned Agent:', ticket_info.get('assigned_to', 'Unassigned') or 'Unassigned'],
        ['Customer:', ticket_info.get('customers', 'Unknown') or 'Unknown'],
    ]
    
    table = Table(data, colWidths=[1.5*inch, 4.5*inch])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#6b7280')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#1f2937')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
    ]))
    
    elements.append(table)
    elements.append(Spacer(1, 10))
    
    return elements


def create_timeline_section(styles, ticket_info: dict) -> list:
    """Create the timeline section."""
    elements = []
    
    elements.append(Paragraph("🕐 TIMELINE", styles['SectionHeader']))
    
    created = format_date(ticket_info.get('date_ticket_created', ''))
    closed = format_date(ticket_info.get('date_closed', ''))
    
    data = [
        ['Created:', created],
        ['Closed:', closed],
    ]
    
    table = Table(data, colWidths=[1.5*inch, 4.5*inch])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#6b7280')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#1f2937')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.HexColor('#059669')),  # Created - green
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
    ]))
    
    elements.append(table)
    elements.append(Spacer(1, 20))
    
    return elements


def create_messages_section(styles, messages: list) -> list:
    """Create the conversation history section."""
    elements = []
    
    elements.append(HRFlowable(
        width="100%", 
        thickness=1, 
        color=colors.HexColor('#e5e7eb'),
        spaceBefore=10,
        spaceAfter=20
    ))
    
    msg_count = len(messages)
    elements.append(Paragraph(f"💬 CONVERSATION HISTORY ({msg_count} messages)", styles['SectionHeader']))
    
    if not messages:
        elements.append(Paragraph("No messages recorded for this ticket.", styles['Value']))
        return elements
    
    for i, msg in enumerate(messages):
        # Message container
        sender = clean_text_for_pdf(msg.get('action_creator_name', 'Unknown'))
        role = msg.get('role', 'Customer')
        timestamp = format_date(msg.get('date_action_created', ''))
        
        # Get message body - prefer cleaned_description
        body = msg.get('cleaned_description', '') or msg.get('action_description', '') or ''
        body = clean_text_for_pdf(body)
        
        # Truncate very long messages for PDF (keep first 3000 chars)
        if len(body) > 3000:
            body = body[:3000] + "... [Message truncated]"
        
        # Role badge
        is_agent = role == 'Agent'
        role_text = "🔷 AGENT" if is_agent else "○ CUSTOMER"
        role_style = styles['AgentBadge'] if is_agent else styles['CustomerBadge']
        
        # Create message block
        elements.append(Paragraph(role_text, role_style))
        elements.append(Paragraph(sender, styles['MessageSender']))
        elements.append(Paragraph(timestamp, styles['MessageTime']))
        
        # Message body - handle line breaks
        if body:
            # Split by newlines and create paragraphs
            lines = body.split('\n')
            for line in lines:
                if line.strip():
                    elements.append(Paragraph(line, styles['MessageBody']))
                else:
                    elements.append(Spacer(1, 6))
        else:
            elements.append(Paragraph("[No message content]", styles['MessageBody']))
        
        # Add separator between messages (except last)
        if i < len(messages) - 1:
            elements.append(Spacer(1, 10))
            elements.append(HRFlowable(
                width="100%", 
                thickness=0.5, 
                color=colors.HexColor('#e5e7eb'),
                spaceBefore=5,
                spaceAfter=15
            ))
    
    return elements


def add_page_number(canvas, doc):
    """Add page number and footer to each page."""
    page_num = canvas.getPageNumber()
    text = f"Page {page_num}"
    
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    canvas.setFillColor(colors.HexColor('#9ca3af'))
    
    # Page number on right
    canvas.drawRightString(7.5 * inch, 0.5 * inch, text)
    
    # Footer text on left
    canvas.drawString(1 * inch, 0.5 * inch, "TeamSupport Archive • Confidential")
    
    canvas.restoreState()


def generate_ticket_pdf(ticket_info: dict, messages: list) -> io.BytesIO:
    """
    Generate a professional PDF summary for a support ticket.
    
    Args:
        ticket_info: Dictionary containing ticket metadata
        messages: List of message dictionaries
    
    Returns:
        BytesIO buffer containing the PDF
    """
    buffer = io.BytesIO()
    
    # Create document
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=1*inch,
        leftMargin=1*inch,
        topMargin=1*inch,
        bottomMargin=1*inch,
        title=f"Ticket #{ticket_info.get('ticket_number', 'Unknown')} Summary",
        author="TeamSupport Archive"
    )
    
    styles = get_styles()
    elements = []
    
    # Build PDF sections
    elements.extend(create_header_section(styles, ticket_info))
    elements.extend(create_overview_section(styles, ticket_info))
    elements.extend(create_people_section(styles, ticket_info))
    elements.extend(create_timeline_section(styles, ticket_info))
    elements.extend(create_messages_section(styles, messages))
    
    # Build PDF with page numbers
    doc.build(elements, onFirstPage=add_page_number, onLaterPages=add_page_number)
    
    buffer.seek(0)
    return buffer


if __name__ == '__main__':
    # Test with sample data
    sample_ticket = {
        'ticket_number': 12345,
        'ticket_name': 'Sample Support Request - Testing PDF Generation',
        'status': 'Resolved',
        'ticket_type': 'Support',
        'subcategory': 'Technical Issue',
        'ticket_source': 'Email',
        'ticket_owner': 'John Smith',
        'assigned_to': 'Nadia Clark',
        'customers': 'Acme Corporation',
        'date_ticket_created': '2024-12-15 09:30:00',
        'date_closed': '2024-12-16 14:45:00',
    }
    
    sample_messages = [
        {
            'action_creator_name': 'John Smith',
            'role': 'Customer',
            'date_action_created': '2024-12-15 09:30:00',
            'cleaned_description': 'Hello, I need help with my account.\n\nI cannot log in and it keeps showing an error message.',
        },
        {
            'action_creator_name': 'Nadia Clark',
            'role': 'Agent',
            'date_action_created': '2024-12-15 10:15:00',
            'cleaned_description': 'Hi John,\n\nThank you for reaching out. I would be happy to help you with this issue.\n\nCan you please provide the exact error message you are seeing?',
        },
    ]
    
    pdf_buffer = generate_ticket_pdf(sample_ticket, sample_messages)
    
    with open('test_ticket.pdf', 'wb') as f:
        f.write(pdf_buffer.read())
    
    print("✅ Test PDF generated: test_ticket.pdf")
