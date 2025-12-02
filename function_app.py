import azure.functions as func
import logging
import os
import json
import requests
import pyodbc

app = func.FunctionApp()

@app.route(route="http_trigger_test", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger_test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
@app.route(route="ingest_toggl_data", auth_level=func.AuthLevel.FUNCTION)
def ingest_toggl_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting Toggl Data Ingestion.')

    # --- 1. Configuration ---
    try:
        TOGGL_API_TOKEN = os.environ["TOGGL_API_TOKEN"]
        WORKSPACE_ID = os.environ["TOGGL_WORKSPACE_ID"]
        SQL_CONN_STRING = os.environ["SqlConnectionString"]
        
        AUTH = (TOGGL_API_TOKEN, "api_token")
        URL = f"https://api.track.toggl.com/reports/api/v3/workspace/{WORKSPACE_ID}/search/time_entries"
    except KeyError as e:
        return func.HttpResponse(f"Missing config: {e}", status_code=500)

    # --- 2. Get Dates from the ADF Request ---
    try:
        req_body = req.get_json()
        start_date = req_body.get('start_date')
        end_date = req_body.get('end_date')
        if not start_date or not end_date:
            raise ValueError("Missing start_date or end_date")
    except ValueError as e:
        return func.HttpResponse(f"Invalid Body: {e}", status_code=400)

    conn = None
    try:
        # --- 3. Connect to SQL & Truncate Staging ---
        conn = pyodbc.connect(SQL_CONN_STRING)
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[StageTogglTimeEntries]")
        conn.commit()

        # --- 4. The Pagination Loop ---
        first_row_number = 1
        total_loaded = 0
        
        while True:
            # Construct Payload
            payload = {
                "start_date": start_date,
                "end_date": end_date,
                "page_size": 50,
                "first_row_number": first_row_number
            }

            # Call API
            response = requests.post(URL, json=payload, auth=AUTH)
            response.raise_for_status()
            data = response.json()

            # Break if no data returned
            if not data:
                break

            # --- 5. Flatten and Prepare Data ---
            rows_to_insert = []
            for entry in data:
                # Guard against empty time_entries list
                if not entry.get('time_entries'):
                    continue
                
                # We grab the FIRST item in the sub-list
                details = entry['time_entries'][0]
                # FIX #1: Convert Billable to 1 or 0 (Safer for SQL)
                is_billable = 1 if entry.get('billable') else 0

                # Map JSON fields to SQL Columns
                row = (
                    details.get('id'),                # toggl_time_entry_id
                    entry.get('user_id'),             # user_id
                    entry.get('project_id'),          # project_id
                    entry.get('task_id'),             # task_id
                    entry.get('billable'),            # billable
                    entry.get('description'),         # description
                    details.get('start'),             # start_time
                    details.get('stop'),              # stop_time
                    details.get('seconds'),           # duration_seconds
                    details.get('at')                 # updated_at
                )
                rows_to_insert.append(row)

            # --- 6. Bulk Insert into Staging ---
            if rows_to_insert:
                sql_insert = """
                    INSERT INTO [dbo].[StageTogglTimeEntries] (
                        toggl_time_entry_id, user_id, project_id, task_id, billable,
                        description, start_time, stop_time, duration_seconds, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                #cursor.fast_executemany = True
                cursor.executemany(sql_insert, rows_to_insert)
                conn.commit()
                total_loaded += len(rows_to_insert)

            # --- 7. Handle Pagination ---
            next_row = response.headers.get('X-Next-Row-Number')
            if next_row:
                first_row_number = int(next_row)
            else:
                break # No more pages

        return func.HttpResponse(
            f"Success. Loaded {total_loaded} records into Staging.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error processing batch: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)

    finally:
        # This runs NO MATTER WHAT (Success or Error)
        if conn:
            conn.close() 