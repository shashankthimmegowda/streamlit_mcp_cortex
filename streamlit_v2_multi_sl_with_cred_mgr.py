# streamlit_app.py — NL prompt → infer SL params → compiled SQL → run
import json, re, uuid, requests, pandas as pd
import streamlit as st
import _snowflake
from snowflake.snowpark.context import get_active_session

# ── Debug Configuration ────────────────────────────────────────────────────────
DEBUG_MODE = False  # Set to True to enable debug outputs

# ──────────────────────────────────────────────────────────────────────────────────────────
# ── Config ────────────────────────────────────────────────────────────────────
# These are just the default values that are used the first time the app runs (or the creds table doesn't exist)
DEFAULT_MCP_URL = "https://cloud.getdbt.com/api/ai/v1/mcp/" #default
DEFAULT_TARGET_DATABASE = "ANALYTICS"
DEFAULT_TARGET_SCHEMA = "PROD"
DEFAULT_DBT_TOKEN = '<ADD TOKEN FROM dbt SL/Service Account>'
DEFAULT_DBT_PROD_ENV_ID = '<ADD DBT PROD ACCOUNT ID>'

LLM_ENDPOINT = "/api/v2/cortex/inference:complete"
LLM_TIMEOUT = 50000  # in milliseconds
# ── end of Config ────────────────────────────────────────────────────────────────────
# ──────────────────────────────────────────────────────────────────────────────────────────

MCP_TOOL_LIST = ['list_metrics', 'get_dimensions', 'get_entities', 'query_metrics']

session = get_active_session()
try:
    session.sql("ALTER SESSION SET QUERY_TAG = 'SL_MCP_CHAT_APP'").collect()
    # Not needed unelss we want to diretly use the database and schema in the snowflake session (dbt handles all queries)
    # if we use this - it should be later on after loading credentials
    # session.sql(f"USE DATABASE {TARGET_DATABASE}").collect()
    # session.sql(f"USE SCHEMA {TARGET_DATABASE}.{TARGET_SCHEMA}").collect()
except Exception:
    pass


# ─── Snowflake LLM call ───────────────────────────────────────────────────────────────────────────
def call_snowflake_llm(messages_list):
    """
    Make an API call to Snowflake's LLM integration.
    
    Args:
        messages_list (list): The conversation messages
    
    Returns:
        tuple: (text, tool_use_id, tool_name, tool_input_json)
    """
    text = ""
    tool_name = None
    tool_use_id = None
    tool_input = ""
    tool_input_json = None
    
    payload = {
        "model": "claude-3-5-sonnet",
        # "model": "snowflake-arctic",
        "messages": messages_list,
        "tool_choice": {
            "type": "auto",
            "name": MCP_TOOL_LIST
        },
        "tools": st.session_state["tools"]
    }

         # Add debug logging
    if DEBUG_MODE:
         with st.popover("Debug - LLM Payload"):
             st.write("Debug - Payload being sent to API:")
             st.write(json.dumps(payload, indent=2))

    try:
        resp = _snowflake.send_snow_api_request(
            "POST",
            LLM_ENDPOINT,
            {},
            {},
            payload,
            None,
            LLM_TIMEOUT,
        )

        if resp["status"] != 200:
            st.error(f"API Error: {resp}")
            return None, None, None, None

        try:
            response_content = json.loads(resp["content"])
            
            for response in response_content:
                data = response.get('data', {})
                for choice in data.get('choices', []):
                    delta = choice.get('delta', {})
                    content_list = delta.get('content_list', [])
                    
                    for content in content_list:
                        content_type = content.get('type')
                        
                        if content_type == 'text':
                            text += content.get('text', '')
                        elif content_type is None:
                            # Handle tool use based on your original pattern
                            if content.get('tool_use_id'):
                                tool_name = content.get('name')
                                tool_use_id = content.get('tool_use_id')
                            tool_input += content.get('input', '')
                            
            if tool_input != '':
                try:
                    tool_input_json = json.loads(tool_input)
                except json.JSONDecodeError:
                    st.error("Issue with Tool Input")
                    st.error(tool_input)
                    tool_input_json = None
                                    
        except json.JSONDecodeError as e:
            st.error(f"Failed to parse API response: {e}")
            return None, None, None, None
            
        return text, tool_use_id, tool_name, tool_input_json
            
    except Exception as e:
        st.error(f"Error making API request: {str(e)}")
        return None, None, None, None

# ── MCP Credentials Management ────────────────────────────────────────────────────
def create_mcp_credentials_table():
    """Create MCP_CREDENTIALS table if it doesn't exist and insert default connection."""
    try:
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS MCP_CREDENTIALS (
            NAME STRING PRIMARY KEY,
            MCP_URL STRING,
            DBT_TOKEN STRING,
            DBT_PROD_ENV_ID STRING,
            TARGET_DATABASE STRING,
            TARGET_SCHEMA STRING,
            NOTES STRING
        )
        """
        session.sql(create_table_sql).collect()
        
        # Check if default connection exists
        check_sql = "SELECT COUNT(*) as count FROM MCP_CREDENTIALS WHERE NAME = 'Default'"
        result = session.sql(check_sql).collect()
        
        if result[0]['COUNT'] == 0:
            # Insert default connection with current hardcoded values
            insert_sql = """
            INSERT INTO MCP_CREDENTIALS (NAME, MCP_URL, DBT_TOKEN, DBT_PROD_ENV_ID, TARGET_DATABASE, TARGET_SCHEMA, NOTES)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            session.sql(insert_sql, params=[
                'Default',
                DEFAULT_MCP_URL,
                DEFAULT_DBT_TOKEN,
                DEFAULT_DBT_PROD_ENV_ID,
                DEFAULT_TARGET_DATABASE,
                DEFAULT_TARGET_SCHEMA,
                'Default MCP connection'
            ]).collect()
            
    except Exception as e:
        st.error(f"Error creating MCP credentials table: {str(e)}")

def load_mcp_credentials(connection_name: str) -> dict | None:
    """Load MCP credentials from table by connection name."""
    try:
        sql = "SELECT * FROM MCP_CREDENTIALS WHERE NAME = ?"
        result = session.sql(sql, params=[connection_name]).collect()
        
        if result:
            row = result[0]
            return {
                'CONNECTION_NAME': row['NAME'],
                'MCP_URL': row['MCP_URL'],
                'DBT_TOKEN': row['DBT_TOKEN'],
                'DBT_PROD_ENV_ID': row['DBT_PROD_ENV_ID'],
                'TARGET_DATABASE': row['TARGET_DATABASE'],
                'TARGET_SCHEMA': row['TARGET_SCHEMA'],
                'CONNECTION_NOTES': row['NOTES']
            }
        return None
    except Exception as e:
        st.error(f"Error loading MCP credentials: {str(e)}")
        return None

def set_mcp_credentials():
    """Callback function to handle MCP connection changes."""
    selected_connection = st.session_state.get('selected_connection')
    current_connection = st.session_state.get('current_connection', 'None')
    
    if DEBUG_MODE:
        with st.popover("Setting MCP credentials"):
            st.write(f"Selected connection: {selected_connection}")
            st.write(f"Current connection: {current_connection}")
    if selected_connection != current_connection:
        st.session_state.current_connection = selected_connection
        
        # Load credentials for selected connection
        credentials = load_mcp_credentials(selected_connection)
        if credentials:
            # Update session state variables
            st.session_state['CONNECTION_NAME'] = credentials['CONNECTION_NAME']
            st.session_state['MCP_URL'] = credentials['MCP_URL']
            st.session_state['DBT_TOKEN'] = credentials['DBT_TOKEN']
            st.session_state['DBT_PROD_ENV_ID'] = credentials['DBT_PROD_ENV_ID']
            st.session_state['TARGET_DATABASE'] = credentials['TARGET_DATABASE']
            st.session_state['TARGET_SCHEMA'] = credentials['TARGET_SCHEMA']
            st.session_state['CONNECTION_NOTES'] = credentials['CONNECTION_NOTES']
            
            # Update headers with new token and environment
            st.session_state['HEADERS'] = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "Authorization": f"token {st.session_state['DBT_TOKEN']}",
                "x-dbt-prod-environment-id": st.session_state['DBT_PROD_ENV_ID'],
            }
            
            # Reset MCP tools loading flag to force reload
            st.session_state['is_mcp_loaded'] = 0
            
            # Clear conversation history when switching connections
            st.session_state.messages = []
            st.session_state.conversation_messages = []

            get_available_connections.clear()
            sl_metrics_catalog.clear()
            sl_dimensions_for.clear()
            sl_entities_for.clear()

            
            # Update Snowflake session to use new database/schema
            try:
                session.sql(f"USE DATABASE {st.session_state['TARGET_DATABASE']}").collect()
                session.sql(f"USE SCHEMA {st.session_state['TARGET_DATABASE']}.{st.session_state['TARGET_SCHEMA']}").collect()
            except Exception:
                pass

def create_new_connection():
    """Handler function to set up a new connection with default values."""
    st.session_state['CONNECTION_NAME'] = "New Connection"
    st.session_state['MCP_URL'] = DEFAULT_MCP_URL
    st.session_state['DBT_TOKEN'] = ""
    st.session_state['DBT_PROD_ENV_ID'] = ""
    st.session_state['TARGET_DATABASE'] = ""
    st.session_state['TARGET_SCHEMA'] = ""
    st.session_state['CONNECTION_NOTES'] = ""


def save_mcp_credentials(name: str, mcp_url: str, dbt_token: str, dbt_prod_env_id: str, 
                        target_database: str, target_schema: str, notes: str = ""):
    """Save MCP credentials to table."""
    try:
        # Use MERGE to insert or update
        merge_sql = """
        MERGE INTO MCP_CREDENTIALS AS target
        USING (SELECT ? as NAME, ? as MCP_URL, ? as DBT_TOKEN, ? as DBT_PROD_ENV_ID, 
                      ? as TARGET_DATABASE, ? as TARGET_SCHEMA, ? as NOTES) AS source
        ON target.NAME = source.NAME
        WHEN MATCHED THEN 
            UPDATE SET MCP_URL = source.MCP_URL, DBT_TOKEN = source.DBT_TOKEN, 
                      DBT_PROD_ENV_ID = source.DBT_PROD_ENV_ID, TARGET_DATABASE = source.TARGET_DATABASE,
                      TARGET_SCHEMA = source.TARGET_SCHEMA, NOTES = source.NOTES
        WHEN NOT MATCHED THEN 
            INSERT (NAME, MCP_URL, DBT_TOKEN, DBT_PROD_ENV_ID, TARGET_DATABASE, TARGET_SCHEMA, NOTES)
            VALUES (source.NAME, source.MCP_URL, source.DBT_TOKEN, source.DBT_PROD_ENV_ID, 
                   source.TARGET_DATABASE, source.TARGET_SCHEMA, source.NOTES)
        """
        session.sql(merge_sql, params=[name, mcp_url, dbt_token, dbt_prod_env_id, 
                                      target_database, target_schema, notes]).collect()
        get_available_connections.clear()
        return True
    except Exception as e:
        st.error(f"Error saving MCP credentials: {str(e)}")
        return False

@st.cache_data(ttl=300)
def get_available_connections() -> list[str]:
    """Get list of available MCP connection names, with 'Default' first if present."""
    try:
        sql = "SELECT NAME FROM MCP_CREDENTIALS ORDER BY NAME"
        result = session.sql(sql).collect()
        names = [row['NAME'] for row in result]
        if 'Default' in names:
            names.remove('Default')
            return ['Default'] + names
        return names
    except Exception as e:
        st.error(f"Error getting available connections: {str(e)}")
        return ['Default']

# ── dbt MCP helpers ───────────────────────────────────────────────────────────────
def _jsonrpc(method: str, params: dict | None = None) -> dict:
    payload = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params or {}}
    r = requests.post(st.session_state['MCP_URL'], headers=st.session_state['HEADERS'], json=payload, timeout=60)
    r.raise_for_status()
    
    # Read the response as text
    response_text = r.text

    if DEBUG_MODE:
        with st.expander("Initial response text:"):
            st.write(response_text)

    # Split into individual events (SSE format uses double newlines)
    events = response_text.split('\n\n')

    for event in events:
        if event.strip():
            lines = event.split('\n')
            for line in lines:
                if line.startswith('data: '):
                    data_content = line[6:]  # Remove 'data: ' prefix
                    if data_content.strip():
                        try:
                            # Parse the JSON data
                            json_data = json.loads(data_content)
                            if DEBUG_MODE:
                                with st.expander("Parsed data:"):
                                    st.write(json_data)
                            return json_data.get("result", [])
                        except json.JSONDecodeError:
                            st.warn(f"Non-JSON data: {data_content}")

def mcp_call(tool: str, arguments: dict) -> dict:
    result = _jsonrpc("tools/call", {"name": tool, "arguments": arguments})
    if result.get("isError"):
        msg = "\n".join(c.get("text", "") for c in result.get("content", []) if c.get("type") == "text").strip()
        raise RuntimeError(msg or "MCP tool error")
    return result

def extract_text(res: dict) -> str:
    return "\n".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text").strip()

# ── Semantic Layer helpers ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def sl_metrics_catalog() -> list[dict]:
    resp = mcp_call("list_metrics", {})
    try:
        metrics = []
        for item in resp["content"]:
            if item.get("type") == "text" and item.get("text"):
                try:
                    metric_data = json.loads(item["text"])
                    metrics.append(metric_data)
                except json.JSONDecodeError:
                    st.error(f"Failed to parse metric JSON: {item['text']}")
        return metrics
    except Exception as e:
        st.error("Could not read content from list_metrics tool call. Error: " + str(e))
        return []

@st.cache_data(ttl=300)
def sl_dimensions_for(metrics: list[str]) -> list[str]:
    if not metrics:
        return []
    txt = extract_text(mcp_call("get_dimensions", {"metrics": metrics}))
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

@st.cache_data(ttl=300)
def sl_entities_for(metrics: list[str]) -> list[str]:
    if not metrics:
        return []
    txt = extract_text(mcp_call("get_entities", {"metrics": metrics}))
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def format_query_results(results: dict, compiled_sql: str | None = None) -> str:
    """Format query results and optional compiled SQL into a readable string."""
    output_parts = []
    
    # Add the results
    if isinstance(results, list):
        # Format as table if we have data
        if results:
            # Get headers from first row
            headers = list(results[0].keys())
            # Format each row
            rows = [
                " | ".join(str(row.get(h, '')) for h in headers)
                for row in results
            ]
            output_parts.extend([
                "Results:",
                " | ".join(headers),
                "-" * (sum(len(h) for h in headers) + 3 * (len(headers) - 1)),
                *rows
            ])
        else:
            output_parts.append("No results returned.")
    else:
        output_parts.append("Unexpected results format.")
        if DEBUG_MODE:
            output_parts.append(f"Raw results: {results}")
    
    # Add the compiled SQL if available
    if compiled_sql:
        output_parts.extend([
            "",
            "Compiled SQL:",
            "```sql",
            compiled_sql,
            "```"
        ])
    
    return "\n".join(output_parts)

def sl_compile_sql(metrics: list[str],
                   group_by: list[str] | list[dict] | None = None,
                   where: str | None = None,
                   order_by: list[dict] | None = None,
                   limit: int | None = None) -> str:
    args: dict = {"metrics": metrics}
    if group_by: args["group_by"] = group_by
    if where:    args["where"]    = where
    if order_by: args["order_by"] = order_by
    if limit:    args["limit"]    = int(limit)
    return extract_text(mcp_call("get_metrics_compiled_sql", args)).strip()

def load_mcp_tools():
    # only load tools once per connection
    if 'is_mcp_loaded' not in st.session_state or st.session_state.get('is_mcp_loaded', 0) == 0:
        try:
            st.session_state["tools"] = []
            mcp_tools_init = _jsonrpc("tools/list", {}).get("tools", [])
            if DEBUG_MODE:
                with st.expander("Initial tools response from dbt mcp:"):
                    st.write(mcp_tools_init)
            mcp_tools_filtered = [tool for tool in mcp_tools_init if tool['name'] in MCP_TOOL_LIST]
            mcp_tools_formatted = []

            for t in mcp_tools_filtered:
                new_tool = {
                    "tool_spec": {
                        "type": "generic",
                        "name": t["name"],
                        "description": t["description"],
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "metrics": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "List of metric names to query"
                                },
                                "group_by": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "type": {"type": "string", "enum": ["dimension", "time_dimension", "entity"]},
                                            "grain": {"type": ["string", "null"]}
                                        }
                                    },
                                    "description": "Optional list of dimensions and entities to group by"
                                },
                                "order_by": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "descending": {"type": "boolean"}
                                        }
                                    },
                                    "description": "Optional list of fields to order by"
                                },
                                "where": {
                                    "type": "string",
                                    "description": "Optional WHERE clause using {{ Dimension() }} or {{ Entity() }} syntax"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Optional limit on number of results"
                                }
                            },
                            "required": ["metrics"]
                        } if t["name"] == "query_metrics" else t["inputSchema"]
                    }
                }
                mcp_tools_formatted.append(new_tool)
                
            st.session_state["tools"] = mcp_tools_formatted
            st.session_state["is_mcp_loaded"] = 1

            if DEBUG_MODE:
                with st.expander("Formatted and filtered tools list & spec:"):
                    st.write(st.session_state["tools"])

        except Exception as e:
            st.error(str(e))

# ── Safety helpers ────────────────────────────────────────────────────────────
def is_read_only_select(sql: str) -> bool:
    s = re.sub(r"/\*.*?\*/", "", sql, flags=re.S).strip().lower()
    if not s.startswith(("select", "with")):
        return False
    banned = [" insert ", " update ", " delete ", " merge ", " create ", " replace ",
              " alter ", " drop ", " truncate ", " grant ", " revoke ", " call ", " copy "]
    return not any(b in s for b in banned)

def references_target(sql: str) -> bool:
    return f"{st.session_state['TARGET_DATABASE']}.{st.session_state['TARGET_SCHEMA']}".lower() in sql.lower()

# ── NL → SL inference ─────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

def pick_metric_names_from_text(text: str, catalog: list[dict]) -> list[str]:
    t = _norm(text)
    names = [m.get("name","") for m in catalog if m.get("name")]
    labels = { _norm(m.get("label","")): m.get("name","") for m in catalog if m.get("label") }
    chosen: list[str] = []
    for n in names:
        if _norm(n) and _norm(n) in t:
            chosen.append(n)
    for lbl, real in labels.items():
        if lbl and lbl in t and real not in chosen:
            chosen.append(real)
    return list(dict.fromkeys(chosen))[:3]

def guess_time_grain(text: str) -> str | None:
    t = text.lower()
    if "week" in t:    return "WEEK"
    if "month" in t:   return "MONTH"
    if "quarter" in t: return "QUARTER"
    if "year" in t or "yoy" in t: return "YEAR"
    return None

def infer_group_by_dims(text: str, valid_dims: list[str]) -> list[str]:
    """Pick non-time dims mentioned in text + metric_time@grain if available."""
    t = _norm(text)
    dims_out: list[str] = []
    # time
    grain = guess_time_grain(text)
    if "metric_time" in {d.lower() for d in valid_dims} and grain:
        dims_out.append(f"metric_time@{grain}")
    # other dims from words
    normalized_valid = { _norm(d): d for d in valid_dims if d }
    for token, original in normalized_valid.items():
        if token and token != "metric_time" and token in t:
            dims_out.append(original)
    # de-dup, preserve order
    return list(dict.fromkeys(dims_out))[:5]

def infer_topk(text: str) -> tuple[int | None, bool]:
    """Returns (k, descending_for_metric) if 'top K' or 'bottom K' detected."""
    m = re.search(r"\btop\s+(\d+)\b", text.lower())
    if m: return int(m.group(1)), True
    m = re.search(r"\bbottom\s+(\d+)\b", text.lower())
    if m: return int(m.group(1)), False
    return None, True

def infer_year_filter(text: str) -> tuple[str | None, str | None]:
    """Very light 'in 2024' → date range on metric_time@YEAR. Returns (where, grain_used)"""
    m = re.search(r"\bin\s+(20\d{2})\b", text.lower())
    if not m:
        return None, None
    year = int(m.group(1))
    where = (
        "{{ TimeDimension('metric_time', 'YEAR') }} >= "
        f"'{year}-01-01' AND {{ TimeDimension('metric_time', 'YEAR') }} < '{year+1}-01-01'"
    )
    return where, "YEAR"

def normalize_group_by(dims: list[str]) -> list[str | dict]:
    out: list[str | dict] = []
    for d in dims:
        if "@" in d:
            name, grain = d.split("@", 1)
            out.append({"name": name.strip(), "grain": grain.strip()})
        else:
            out.append(d.strip())
    return out


def main():
    st.title("dbt MCP Demo")

    # Create credentials table if it doesn't exist
    create_mcp_credentials_table()

    with st.sidebar:
        st.subheader("MCP Connection")
        
        # Get available connections
        available_connections = get_available_connections()

         # set initial value of current connection
        if 'current_connection' not in st.session_state:
            st.session_state['current_connection'] = 'None'
        
        # Connection dropdown
        st.selectbox(
            "Select MCP Connection:",
            available_connections,
            index=available_connections.index(st.session_state.current_connection) if st.session_state.current_connection in available_connections else 0,
            key='selected_connection',
            on_change=set_mcp_credentials
        )
        
        # Initialize credentials if not already loaded
        if 'MCP_URL' not in st.session_state:
            # Load credentials for the first time
            initial_connection = st.session_state.get('selected_connection', 'Default')
            if initial_connection in available_connections:
                credentials = load_mcp_credentials(initial_connection)
                if credentials:
                    st.session_state['CONNECTION_NAME'] = credentials['CONNECTION_NAME']
                    st.session_state['MCP_URL'] = credentials['MCP_URL']
                    st.session_state['DBT_TOKEN'] = credentials['DBT_TOKEN']
                    st.session_state['DBT_PROD_ENV_ID'] = credentials['DBT_PROD_ENV_ID']
                    st.session_state['TARGET_DATABASE'] = credentials['TARGET_DATABASE']
                    st.session_state['TARGET_SCHEMA'] = credentials['TARGET_SCHEMA']
                    st.session_state['CONNECTION_NOTES'] = credentials['CONNECTION_NOTES']
                    
                    # Update headers with new token and environment
                    st.session_state['HEADERS'] = {
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        "Authorization": f"token {st.session_state['DBT_TOKEN']}",
                        "x-dbt-prod-environment-id": st.session_state['DBT_PROD_ENV_ID'],
                    }
                    
                    st.session_state.current_connection = initial_connection

    # Load MCP tools for current connection
    load_mcp_tools()

    with st.sidebar:
        if st.session_state.get('tools'):
            st.success(f"Connected to MCP ✓ ({st.session_state.current_connection})")
            
        if st.button("Check MCP SL tools"):
            st.json([t.get("tool_spec").get("name") for t in st.session_state["tools"]])
        
        st.divider()
        
        # Connection Management
        with st.expander("Manage Connections"):
            st.subheader("Add Connection")
            

            is_save_connection_disabled = True if st.session_state['CONNECTION_NAME'] == 'Default' else False
            # Form for adding/editing connections
            with st.form("connection_form"):
                conn_name = st.text_input("Connection Name", value = st.session_state['CONNECTION_NAME'])
                mcp_url = st.text_input("MCP URL", value = st.session_state['MCP_URL'])
                dbt_token = st.text_input("DBT Token", type="password")
                dbt_env_id = st.text_input("DBT Environment ID", value = st.session_state['DBT_PROD_ENV_ID'])
                target_db = st.text_input("Target Database", value = st.session_state['TARGET_DATABASE'])
                target_schema = st.text_input("Target Schema", value = st.session_state['TARGET_SCHEMA'])
                notes = st.text_area("Notes", value = st.session_state['CONNECTION_NOTES'])
                
                if st.form_submit_button("Save Connection", disabled=is_save_connection_disabled):
                    if conn_name and mcp_url and dbt_token and dbt_env_id and target_db and target_schema:
                        if save_mcp_credentials(conn_name, mcp_url, dbt_token, dbt_env_id, target_db, target_schema, notes):
                            set_mcp_credentials()
                            st.success(f"Connection '{conn_name}' saved successfully!")
                            st.rerun()
                        else:
                            st.error("Failed to save connection")
                if is_save_connection_disabled:
                    st.warning("⚠️ You cannot modify the Default connection. Create a new connection instead.")
            
            # New Connection button
            if st.button("🆕 New Connection", 
                        help="Create a new connection with default values",
                        use_container_width=True):
                create_new_connection()
                st.rerun()
            
    # Initialize session state properly
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    # Initialize conversation messages for API
    if 'conversation_messages' not in st.session_state:
        st.session_state.conversation_messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message['role']):
            st.write(message['content'])
            # Show weather icon if it exists
            if 'icon' in message:
                st.image(message['icon'])

    if query := st.chat_input("Ask me about your semantic layer metrics!"):
        # Add user message to chat
        with st.chat_message("user"):
            st.write(query)
        st.session_state.messages.append({"role": "user", "content": query})
        
        # Add to conversation messages for API (ensure content is never empty)
        user_message = {"role": "user", "content": query}
        st.session_state.conversation_messages.append(user_message)

        # Add system message to encourage completing tasks
        system_message = {
            "role": "system",
            "content": "You are a helpful assistant that can use dbt Semantic Layer tools to answer business questions. When asked a question that requires data analysis, use the available tools to get the information needed. If you need to make multiple tool calls to complete a task, do them in sequence within your response. Always provide the final answer based on the tool results."
        }
        if not any(msg.get("role") == "system" for msg in st.session_state.conversation_messages):
            st.session_state.conversation_messages.insert(0, system_message)
        
        # Process conversation with potential multiple tool calls
        max_iterations = 5  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            with st.spinner(f"Processing your request... ({iteration}/{max_iterations})"):
                text, tool_use_id, tool_name, tool_input_json = call_snowflake_llm(st.session_state.conversation_messages)

            # Debug output
            if DEBUG_MODE:
                 with st.popover("Debug - LLM Response"):
                     st.write(f"text: {text}")
                     st.write(f"tool_use_id: {tool_use_id}")
                     st.write(f"tool_name: {tool_name}")
                     st.write(f"tool_input_json:")
                     st.write(tool_input_json)

            if text is None:
                st.error("Failed to get response from Claude")
                return

            # Display Claude's response
            with st.chat_message("assistant"):
                st.write(text)

            # If no tool was called, we have the final answer
            if not tool_name:
                # Add the final response to session state
                st.session_state.messages.append({"role": "assistant", "content": text})
                st.session_state.conversation_messages.append({
                    "role": "assistant",
                    "content": text if text else "I've completed your request."
                })
                break  # Exit the loop since we have the final answer

            # Create assistant message with tool call
            assistant_message = {
                "role": "assistant",
                "content": text if text else ""
            }

            # Add tool use to assistant message
            assistant_message["content_list"] = [
                {
                    "type": "tool_use",
                    "tool_use": {
                        "tool_use_id": tool_use_id,
                        "name": tool_name,
                        "input": {} if tool_input_json is None else tool_input_json
                    }
                }
            ]

            # Add assistant message to conversation (required for tool results to work)
            st.session_state.conversation_messages.append(assistant_message)

            # Process tool call
            if tool_name == 'list_metrics':
                with st.spinner('getting metrics from dbt...'):
                    metrics_list = sl_metrics_catalog()
                
                    # Make sure metrics_list is properly formatted
                    if isinstance(metrics_list, list):
                        metrics_text = "Available Metrics:\n\n" + "\n\n".join([
                            f"• {metric.get('name', '')}\n"
                            f"  Type: {metric.get('type', '')}\n"
                            f"  Label: {metric.get('label', '')}\n"
                            f"  Description: {metric.get('description', '')}"
                            for metric in metrics_list
                        ])
                    else:
                        st.error("metrics_list is not in expected format")
                        st.write("metrics_list type:", type(metrics_list))
                        metrics_text = "Error: Could not format metrics list"
                        
                    content_text = metrics_text

                # Debug the metrics_list
                if DEBUG_MODE:
                    with st.expander("Debug - Metrics List"):
                        st.write("Raw metrics list:")
                        st.write(metrics_list)

            elif tool_name == 'get_dimensions':
                if not tool_input_json or 'metrics' not in tool_input_json:
                    st.error("No metrics provided for dimensions lookup")
                    return
                    
                with st.spinner('getting dimensions for metrics...'):
                    try:
                        dimensions_list = sl_dimensions_for(tool_input_json['metrics'])
                        
                        # Debug the dimensions list
                        if DEBUG_MODE:
                            with st.expander("Debug - Dimensions List"):
                                st.write("Raw dimensions list:")
                                st.write(dimensions_list)
                                st.write("For metrics:", tool_input_json['metrics'])
                        
                        if isinstance(dimensions_list, list):
                            metrics_str = ", ".join(tool_input_json['metrics'])
                            dimensions_text = f"Available Dimensions for metrics [{metrics_str}]:\n\n" + "\n".join([
                                f"• {dim}" for dim in dimensions_list
                            ])
                        else:
                            st.error("dimensions_list is not in expected format")
                            st.write("dimensions_list type:", type(dimensions_list))
                            dimensions_text = "Error: Could not format dimensions list"

                    except Exception as e:
                        st.error(f"Error getting dimensions: {str(e)}")
                        dimensions_text = f"Error retrieving dimensions: {str(e)}"

                    content_text = dimensions_text

            elif tool_name == 'get_entities':
                if not tool_input_json or 'metrics' not in tool_input_json:
                    st.error("No metrics provided for entities lookup")
                    return
                    
                with st.spinner('getting entities for metrics...'):
                    try:
                        entities_list = sl_entities_for(tool_input_json['metrics'])
                        
                        # Debug the entities list
                        if DEBUG_MODE:
                            with st.expander("Debug - Entities List"):
                                st.write("Raw entities list:")
                                st.write(entities_list)
                                st.write("For metrics:", tool_input_json['metrics'])
                        
                        if isinstance(entities_list, list):
                            metrics_str = ", ".join(tool_input_json['metrics'])
                            entities_text = f"Available Entities for metrics [{metrics_str}]:\n\n" + "\n".join([
                                f"• {entity}" for entity in entities_list
                            ])
                        else:
                            st.error("entities_list is not in expected format")
                            st.write("entities_list type:", type(entities_list))
                            entities_text = "Error: Could not format entities list"

                    except Exception as e:
                        st.error(f"Error getting entities: {str(e)}")
                        entities_text = f"Error retrieving entities: {str(e)}"

                    content_text = entities_text

            elif tool_name == 'query_metrics':
                if not tool_input_json or 'metrics' not in tool_input_json:
                    st.error("No metrics provided for query")
                    return
                
                with st.spinner('querying metrics...'):
                    try:
                        # Debug the input parameters
                        if DEBUG_MODE:
                            with st.expander("Debug - Query Parameters"):
                                st.write("Input parameters:")
                                st.write(tool_input_json)
                        
                        # First, verify the metrics exist
                        all_metrics = sl_metrics_catalog()
                        available_metrics = {m.get('name') for m in all_metrics if m.get('name')}
                        requested_metrics = set(tool_input_json['metrics'])
                        
                        if not requested_metrics.issubset(available_metrics):
                            invalid_metrics = requested_metrics - available_metrics
                            st.error(f"Invalid metrics requested: {invalid_metrics}")
                            content_text = f"Error: The following metrics are not available: {', '.join(invalid_metrics)}"
                            return
                        
                        # Validate and fix group_by parameters before compiling SQL
                        group_by = tool_input_json.get('group_by', [])
                        if group_by:
                            # Fix missing grain fields for dimensions that need them
                            fixed_group_by = []
                            for item in group_by:
                                if isinstance(item, dict):
                                    fixed_item = item.copy()
                                    # If this is a time dimension or contains time-related keywords, add default grain
                                    name_lower = item.get('name', '').lower()
                                    if ('time' in name_lower or 'date' in name_lower) and 'grain' not in item:
                                        fixed_item['grain'] = 'DAY'  # Default grain for time dimensions
                                    elif 'type' in item and item['type'] == 'time_dimension' and 'grain' not in item:
                                        fixed_item['grain'] = 'DAY'  # Default grain for time dimensions
                                    else:
                                        # For non-time dimensions, ensure grain is null or not present
                                        fixed_item['grain'] = None
                                    fixed_group_by.append(fixed_item)
                                else:
                                    fixed_group_by.append(item)
                            group_by = fixed_group_by

                        # Get the compiled SQL first
                        compiled_sql = sl_compile_sql(
                            metrics=tool_input_json['metrics'],
                            group_by=group_by,
                            where=tool_input_json.get('where'),
                            order_by=tool_input_json.get('order_by'),
                            limit=tool_input_json.get('limit')
                        )
                        
                        if DEBUG_MODE:
                            with st.expander("Debug - Compiled SQL"):
                                st.code(compiled_sql, language="sql")
                        
                        # Execute the query with validated parameters
                        query_params = tool_input_json.copy()
                        if group_by:
                            query_params['group_by'] = group_by

                        try:
                            results = mcp_call("query_metrics", query_params)
                        except Exception as api_error:
                            if "grain" in str(api_error) or "validation" in str(api_error).lower():
                                # If it's still a grain validation error, try with simpler parameters
                                st.warning("Adjusting query parameters to fix validation error...")
                                simple_params = {
                                    "metrics": tool_input_json['metrics'],
                                    "limit": tool_input_json.get('limit', 10)
                                }
                                if tool_input_json.get('where'):
                                    simple_params['where'] = tool_input_json['where']
                                results = mcp_call("query_metrics", simple_params)
                                compiled_sql = "Query executed with simplified parameters due to validation constraints."
                            else:
                                raise api_error
                        
                        if DEBUG_MODE:
                            with st.expander("Debug - Query Results"):
                                st.write("Raw results:")
                                st.write(results)
                        
                        # Format the results and SQL into a readable format
                        content_text = format_query_results(
                            results.get('content', []),
                            compiled_sql
                        )
                        
                    except Exception as e:
                        st.error(f"Error executing query: {str(e)}")
                        content_text = f"Error executing query: {str(e)}"

            # Add tool result message using your original format
            tool_result_message = {
                    'role': 'user',
                    'content': f"Tool result for {tool_name}: {content_text[:100]}...",  # Include summary in content
                    'content_list': [
                        {
                            'type': 'tool_results',
                            'tool_results': {
                                'tool_use_id': tool_use_id,
                                'name': tool_name,
                                'content': [
                                    {
                                        'type': 'text',
                                        'text': content_text
                                    }
                                ]
                            }
                        }
                    ]
                }

            st.session_state.conversation_messages.append(tool_result_message)

            # Get Claude's response after tool results
            with st.spinner("Generating response..."):
                final_text, final_tool_use_id, final_tool_name, final_tool_input = call_snowflake_llm(st.session_state.conversation_messages)

                if final_text:
                    # Store the complete response in session state
                    final_assistant_message = {
                        "role": "assistant",
                        "content": final_text
                    }

                    # Only add to conversation and display if this is truly the final response (no more tool calls)
                    if not final_tool_name:
                        with st.chat_message("assistant"):
                            st.write(final_text)
                        st.session_state.messages.append(final_assistant_message)
                        st.session_state.conversation_messages.append({
                            "role": "assistant",
                            "content": final_text if final_text else "Here are your metrics."
                        })
                        break  # Exit the loop since we have the final answer
                    else:
                        # Claude wants to make another tool call - continue the loop
                        # Don't display this intermediate response in UI
                        text = final_text
                        tool_use_id = final_tool_use_id
                        tool_name = final_tool_name
                        tool_input_json = final_tool_input
                        continue  # Continue the loop for another iteration
                else:
                    # Fallback if final response fails
                    fallback_response = f"Something went wrong. I couldn't retrieve your metrics :("
                    with st.chat_message("assistant"):
                        st.write(fallback_response)

                    fallback_assistant_message = {
                        "role": "assistant",
                        "content": fallback_response
                    }

                    st.session_state.messages.append(fallback_assistant_message)
                    st.session_state.conversation_messages.append({
                        "role": "assistant",
                        "content": fallback_response if fallback_response else "Metric information retrieved."
                    })
                    break

if __name__ == "__main__":
    main()