from flask import Flask, render_template, request, redirect, flash
import uuid
from datetime import datetime
from google.cloud import bigquery
from google import genai
from google.genai import types

app = Flask(__name__)
app.secret_key = "secret-key"

PROJECT_ID = "cloud-professional-services"
DATASET_ID = "sprint"
CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.control_table"
REGION = "us-central1"

bq_client = bigquery.Client(project=PROJECT_ID)
genai_client = genai.Client(project=PROJECT_ID, location=REGION, vertexai=True)

def generate_sql_rule(description, table, column):
    prompt = f"""
    Based on this rule description: "{description}", write a BigQuery SQL query using REGEXP_CONTAINS
    that checks formatting for the column `{column}` in table `{table}`.
    If no clear pattern is found, write a fallback SQL query that checks for common issues (e.g., empty strings).
    Return ONLY the SQL query.
    """
    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text=prompt)]
        )
    ]
    system_instruction = [
        types.Part(text="You're an expert in BigQuery SQL. Always return a single valid SQL query only.")
    ]
    config = types.GenerateContentConfig(
        temperature=0.0,
        max_output_tokens=600,
        system_instruction=system_instruction
    )
    response = genai_client.models.generate_content(
        model="gemini-2.0-flash",
        contents=contents,
        config=config
    )
    if response.candidates and response.candidates[0].content.parts:
        raw_text = response.candidates[0].content.parts[0].text.strip()
        if raw_text.startswith("```sql") and raw_text.endswith("```"):
            raw_text = raw_text[len("```sql"):-len("```")].strip()
        elif raw_text.startswith("```") and raw_text.endswith("```"):
            raw_text = raw_text[len("```"):-len("```")].strip()
        return raw_text
    else:
        return "No content generated."

@app.route('/')
def home():
    return redirect('/create')

@app.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        table = request.form.get("table")
        column = request.form.get("column")
        description = request.form.get("description")
        rule_family = "String Formatting"

        try:
            rule_sql = generate_sql_rule(description, table, column)
            rule_data = {
                "rule_id": str(uuid.uuid4()),
                "source_project_id": PROJECT_ID,
                "source_dataset_id": DATASET_ID,
                "source_table_id": table,
                "metric_column": column,
                "rule_generation_timestamp": datetime.utcnow().isoformat(),
                "rule_sql": rule_sql,
                "rule_family": rule_family,
                "rule_description": description
            }
            errors = bq_client.insert_rows_json(CONTROL_TABLE, [rule_data])
            if errors:
                flash(f"Error inserting rule: {errors}", "danger")
            else:
                flash("Rule successfully added!", "success")
        except Exception as e:
            flash(f"Error: {e}", "danger")
        return redirect('/create')
    return render_template('create.html')

@app.route('/read', methods=['GET'])
def read():
    query = f"""
    SELECT rule_id, source_table_id, metric_column, rule_sql, rule_family, rule_description, rule_generation_timestamp
    FROM `{CONTROL_TABLE}`
    ORDER BY rule_generation_timestamp DESC
    LIMIT 100
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        rules = [dict(row) for row in results]
    except Exception as e:
        flash(f"Error fetching rules: {e}", "danger")
        rules = []
    return render_template('read.html', rules=rules)

@app.route('/update_form', methods=['GET'])
def update_form():
    query = f"""
    SELECT rule_id, source_table_id, metric_column, rule_description
    FROM `{CONTROL_TABLE}`
    ORDER BY rule_generation_timestamp DESC
    LIMIT 100
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        rules = [dict(row) for row in results]
    except Exception as e:
        flash(f"Error fetching rules for update: {e}", "danger")
        rules = []
    return render_template('update.html', rules=rules)

@app.route('/update', methods=['POST'])
def update():
    rule_id = request.form.get("rule_id")
    new_description = request.form.get("description")
    table = request.form.get("table")
    column = request.form.get("column")

    if not all([rule_id, new_description, table, column]):
        flash("Please fill all fields", "warning")
        return redirect('/update_form')

    try:
        new_rule_sql = generate_sql_rule(new_description, table, column)
        update_query = f"""
        UPDATE `{CONTROL_TABLE}`
        SET rule_description = @desc,
            rule_sql = @sql,
            metric_column = @column,
            source_table_id = @table,
            rule_generation_timestamp = @ts
        WHERE rule_id = @rule_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("desc", "STRING", new_description),
                bigquery.ScalarQueryParameter("sql", "STRING", new_rule_sql),
                bigquery.ScalarQueryParameter("column", "STRING", column),
                bigquery.ScalarQueryParameter("table", "STRING", table),
                bigquery.ScalarQueryParameter("ts", "TIMESTAMP", datetime.utcnow()),
                bigquery.ScalarQueryParameter("rule_id", "STRING", rule_id)
            ]
        )
        query_job = bq_client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.num_dml_affected_rows > 0:
            flash("Rule successfully updated!", "success")
        else:
            flash("No rule found with that ID.", "warning")
    except Exception as e:
        flash(f"Error updating rule: {e}", "danger")

    return redirect('/update_form')

@app.route('/delete_form', methods=['GET'])
def delete_form():
    query = f"""
    SELECT rule_id, source_table_id, metric_column, rule_description
    FROM `{CONTROL_TABLE}`
    ORDER BY rule_generation_timestamp DESC
    LIMIT 100
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        rules = [dict(row) for row in results]
    except Exception as e:
        flash(f"Error fetching rules for deletion: {e}", "danger")
        rules = []
    return render_template('delete.html', rules=rules)

@app.route('/delete', methods=['POST'])
def delete():
    rule_id = request.form.get("rule_id")

    if not rule_id:
        flash("Rule ID required to delete.", "warning")
        return redirect('/delete_form')

    delete_query = f"""
    DELETE FROM `{CONTROL_TABLE}`
    WHERE rule_id = @rule_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("rule_id", "STRING", rule_id)
        ]
    )

    try:
        query_job = bq_client.query(delete_query, job_config=job_config)
        query_job.result()
        if query_job.num_dml_affected_rows > 0:
            flash("Rule successfully deleted!", "success")
        else:
            flash("No rule found with that ID.", "warning")
    except Exception as e:
        flash(f"Error deleting rule: {e}", "danger")

    return redirect('/delete_form')

if __name__ == '__main__':
    app.run(debug=True)
