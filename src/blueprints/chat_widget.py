"""
Chat Widget Blueprint
Handles chat widget Q&A interactions
"""

from flask import Blueprint, jsonify, request
from ..logger import get_logger

bp = Blueprint('chat_widget', __name__, url_prefix='/api/chat')
logger = get_logger(__name__)

# Sample Q&A data for testing
QA_DATABASE = {
    "what is ehds": {
        "question": "What is EHDS?",
        "answer": "EHDS stands for European Health Data Space. It's a health-specific ecosystem that aims to promote the exchange of different types of health data to support healthcare delivery, health research, innovation, and policymaking."
    },
    "how do i access tickets": {
        "question": "How do I access tickets?",
        "answer": "You can access tickets by clicking on the 'View Tickets' card on the Support Hub homepage. This will take you to the full support ticket archive where you can search and browse historical tickets."
    },
    "what are canned responses": {
        "question": "What are canned responses?",
        "answer": "Canned responses are pre-written, standardized replies to common support questions. They help support agents respond quickly and consistently to frequently asked questions."
    },
    "how to search tickets": {
        "question": "How do I search for tickets?",
        "answer": "On the tickets page, you can use the search bar to search by ticket number or ticket name. You can also filter tickets by agent, status, category, customer, and date range."
    },
    "who can i contact": {
        "question": "Who can I contact for support?",
        "answer": "For technical support, please reach out to your assigned support agent. You can find their contact information in your ticket details or through the Help Articles section."
    },
    "hello": {
        "question": "Hello",
        "answer": "Hi! How can I help you today? Feel free to ask me about EHDS, tickets, canned responses, or any other support-related questions."
    },
    "hi": {
        "question": "Hi",
        "answer": "Hello! I'm here to help. What would you like to know about the Support Hub?"
    }
}

def find_best_match(user_message: str) -> dict:
    """Find the best matching Q&A based on user message."""
    user_message_lower = user_message.lower().strip()
    
    # Direct keyword matching
    for key, qa in QA_DATABASE.items():
        if key in user_message_lower:
            return qa
    
    # Check if any question words match
    for qa in QA_DATABASE.values():
        if user_message_lower in qa["question"].lower():
            return qa
    
    # Default response
    return {
        "question": user_message,
        "answer": "I'm sorry, I don't have an answer for that question yet. Please try asking about EHDS, tickets, canned responses, or searching for information."
    }

@bp.route('/message', methods=['POST'])
def chat_message():
    """Handle incoming chat messages and return responses."""
    try:
        data = request.get_json()
        
        if not data or 'message' not in data:
            return jsonify({
                'error': 'Message is required'
            }), 400
        
        user_message = data['message']
        logger.info(f"Chat message received: {user_message}")
        
        # Find best matching response
        response = find_best_match(user_message)
        
        return jsonify({
            'success': True,
            'response': response['answer'],
            'timestamp': None  # Frontend will handle timestamp
        })
        
    except Exception as e:
        logger.error(f"Error processing chat message: {str(e)}")
        return jsonify({
            'error': 'An error occurred processing your message'
        }), 500

@bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for the chat widget."""
    return jsonify({
        'status': 'healthy',
        'service': 'chat_widget'
    })
