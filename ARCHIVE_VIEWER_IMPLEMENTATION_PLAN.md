We are moving our ticketing system to a new tool. To ensure our users retain access to their historical data, we need to build a **Read-Only Support Archive**.

I have exported all our past ticket data into a CSV file. I need you to build a simple web interface that parses this CSV and presents it to the user in a clean, readable format ("Inbox" view and "Detail" view).

Below are the specific requirements and logic for processing the data.

### 0. THE TECH STACK
- Backend: Python (Flask)
- Data Processing: Pandas
- Frontend: HTML + Tailwind CSS (I will provide the specific templates below).
- Templating Engine: Jinja2

### 1. DATA PROCESSING LOGIC (app.py)
When the app starts, load the CSV into a Pandas DataFrame.
Perform the following preprocessing:
1.  **Filter:** KEEP rows where 'Is Visible on Hub' is True. DROP everything else.
2.  **Dates:** Convert 'Date Action Created' and 'Date Ticket Created' to datetime objects.
3.  **Grouping:** The CSV is denormalized (one row per message). Group the data by 'Ticket Number'.
    - Create a 'tickets_summary' object for the Index View (unique list of tickets with latest status, subject, etc.).
    - Create a 'ticket_details' dictionary where the key is Ticket ID and value is the list of messages.
4.  **Sorting:** For the ticket details, sort the messages by 'Date Action Created' (Oldest to Newest).
5.  **Role Logic:** 
    - Compare 'Action Creator Name' vs 'Assigned To'. 
    - If they match, mark the message as "Agent". 
    - If they don't match, mark as "Customer".
6.  **Clean Up:** Create a function to clean the message body. If a message starts with "Action added via e-mail", use Regex to strip that header line so only the real email body remains.

### 2. ROUTING
- `/`: Renders 'index.html'. Passes the list of unique tickets. Implements search functionality (filtering by ID or Subject).
- `/ticket/<ticket_id>`: Renders 'detail.html'. Passes the specific conversation history and metadata for that ticket.

### 3. Frontend / UI Requirements
We need two main views. I have created HTML/Tailwind mockups for both to guide the styling.

#### 3.1 View A: The Ticket List (Inbox)
This is the landing page. It should list unique tickets (collapsed from the rows).
*   **Columns:** Ticket ID, Subject, Status, Category, Last Updated Date.
*   **Search:** Users must be able to search by Ticket ID or Ticket Name.
*   **Status Badges:** Color-code statuses (e.g., Green for "Resolved").

**Code Template for List View:**
```html

Use /Users/vieirama/iLab - JSD/TeamSupport/templates/ticket_list.html

```

#### 3.2 View B: The Ticket Detail (Conversation)
When a user clicks a row in the list, show the full history.
*   **Layout:** Two columns (Conversation on the left, Metadata on the right).
*   **Message Formatting:**
    *   The CSV contains newlines (`\n`) in the `Action Description`. These must be preserved (e.g., use CSS `white-space: pre-wrap`).
    *   **Cleanup:** Some rows contain system text like *"Action added via e-mail..."*. If possible, use a regex to strip this header or hide it, so only the actual message body remains.
    *   **Links:** Convert text URLs (e.g., `https://...`) into clickable links.

**Code Template for Detail View:**
```html

Use /Users/vieirama/iLab - JSD/TeamSupport/templates/ticket_detail.html

```

### 4. Deliverables
1.  A web-based viewer that accepts the full CSV dump.
2.  Implementation of the search/filter logic.
3.  Final UI matching the attached HTML mockups.
