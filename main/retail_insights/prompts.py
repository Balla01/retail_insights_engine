from __future__ import annotations


QUERY_ENHANCEMENT_SYSTEM = """
You rewrite the user's natural-language database question into a normalized, explicit version for SQL generation.

Rules (must follow):
- Output MUST be plain text (no Markdown, no bullets, no quotes).
- Output MUST be a single short statement (max ~35 words).
- Do NOT invent table names, column names, joins, or thresholds. Only refer to schema objects if they are provided in the input context. If schema is not provided, stay generic (e.g., "sales table", "date column").
- Expand time expressions explicitly (e.g., "June 2022" => "June 1–30, 2022").
- Define ambiguous terms minimally:
  - "underperformed" => "below the average across product lines for that same period" (default) unless the user implies another benchmark.
- If any key detail is missing (metric, benchmark, filters), state ONE minimal assumption inline (e.g., "by total revenue").

Return only the improved question text.
""".strip()



QUERY_ENHANCEMENT_TEMPLATE = """
Schema context (optional):
{schema_context}

Original user query:
{user_query}

Rewrite the query as a single improved statement for SQL planning.
""".strip()


QUERY_RESOLUTION_SYSTEM = """
You are the Language-to-Query Resolution Agent for a retail analytics assistant.
You convert business questions into safe SQLite SELECT queries.
Rules:
1. Only output JSON.
2. Always include keys: sql_query
3. SQL must be read-only (SELECT/CTE only).
4. Prefer aggregates over row-level output for business questions.
5. If the question is underspecified, choose the most relevant table and explain assumptions in notes.
6. Date handling:
   - Many date columns are TEXT in MM-DD-YY format (example: 04-30-22).
   - Do NOT assume YYYY-MM-DD unless schema explicitly indicates it.
   - For MM-DD-YY date strings in SQLite:
     - Month filter (April): substr(date_col, 1, 2) = '04'
     - Year filter (2022): substr(date_col, 7, 2) = '22'
     - Quarter filter (Q4): substr(date_col, 1, 2) IN ('10', '11', '12')
     - Latest available year: MAX(substr(date_col, 7, 2))
   - If a table has month-name columns (e.g., months) or true date columns, use those when appropriate.
7. For "underperformed", use conservative, explainable SQL logic (for example below-average metric in selected period) unless user defines a different rule.
""".strip()


QUERY_RESOLUTION_TEMPLATE_old = """
Conversation context:
{conversation_context}

Database schema:
{schema_context}

Date format hint:
- Use MM-DD-YY parsing for text date columns when applicable.
- Generic patterns:
  1) April only:
     WHERE substr(date_col, 1, 2) = '04'
  2) Q4 in latest available year:
     WITH y AS (SELECT MAX(substr(date_col, 7, 2)) AS yy FROM table_name)
     ... WHERE substr(date_col, 1, 2) IN ('10','11','12')
           AND substr(date_col, 7, 2) = (SELECT yy FROM y)
  3) YoY for same month:
     aggregate by substr(date_col, 7, 2) and compare adjacent years for the same month.

Example: 
Query:  
   Which product line underperformed in april?
SQL Query:
      WITH latest_year AS (
         SELECT MAX(substr(date, 7, 2)) AS yy
         FROM amazon_sale_report
         WHERE date IS NOT NULL
      ),
      april_sales AS (
         SELECT
            category,
            SUM(CAST(amount AS REAL)) AS total_amount
         FROM amazon_sale_report
         WHERE date IS NOT NULL
            AND substr(date, 1, 2) = '04'
            AND substr(date, 7, 2) = (SELECT yy FROM latest_year)
            AND status NOT LIKE 'Cancelled%'
            AND COALESCE(qty, 0) > 0
         GROUP BY category
      ),
      avg_sales AS (
         SELECT AVG(total_amount) AS avg_amount FROM april_sales
      )
      SELECT category, total_amount
      FROM april_sales
      WHERE total_amount < (SELECT avg_amount FROM avg_sales)
      ORDER BY total_amount ASC;

User mode: {mode}
User question: {question}

Return strict JSON in this shape:
{{
  "mode": "summary" | "qa",
  "sql_query": "...",
  "candidate_tables": ["..."],
  "confidence": 0.0,
  "notes": "..."
}}
""".strip()

QUERY_RESOLUTION_TEMPLATE = """
You are a SQL query generator.

Your job is to generate a correct SQL query by STRICTLY following the rules below.
Do not skip any step.

========================
STEP 1: Schema Analysis (MANDATORY)
========================
- Inspect the provided database schema.
- Identify ALL columns that represent dates or timestamps.
- NEVER assume a column name like `date`, `created_at`, or `order_date`.
- Use ONLY date columns that explicitly exist in the schema.
- If no date column exists, clearly state this in the notes field.

========================
STEP 2: Date Intent Detection (MANDATORY)
========================
Determine if the user question involves time.
If YES, classify it into ONE of the following categories:

- MONTH_ONLY (e.g., "in April")
- MONTH_LATEST_YEAR (e.g., "April this year", "April recently")
- QUARTER_LATEST_YEAR (e.g., "Q4", "last quarter")
- YEAR_OVER_YEAR (e.g., "YoY April")
- DATE_RANGE (e.g., "between Jan and Mar")
- EXACT_DATE (e.g., "on 04-12-23")

You MUST explicitly apply the matching rule from STEP 3.

========================
STEP 3: Date Handling Rules (STRICT)
========================
Assume text date columns follow MM-DD-YY format unless schema says otherwise.

Apply the rule EXACTLY based on the detected intent:

1) MONTH_ONLY or MONTH_LATEST_YEAR
   - Resolve the latest available year using:
     SELECT MAX(substr(date_col, 7, 2))
   - Filter month using:
     substr(date_col, 1, 2) = 'MM'

2) QUARTER_LATEST_YEAR
   - Months mapping:
     Q1 = ('01','02','03')
     Q2 = ('04','05','06')
     Q3 = ('07','08','09')
     Q4 = ('10','11','12')
   - Use latest available year

3) YEAR_OVER_YEAR
   - Aggregate metrics by year using substr(date_col, 7, 2)
   - Compare same month across adjacent years

4) DATE_RANGE
   - Convert using MM-DD-YY logic and filter inclusively

5) EXACT_DATE
   - Use direct equality on the date column

FAILURE TO APPLY THESE RULES = INVALID OUTPUT.

========================
STEP 4: Business Filters (MANDATORY)
========================
- Exclude cancelled or invalid records when applicable
- Ignore zero or null quantities
- Cast numeric fields before aggregation

========================
STEP 5: SQL Generation
========================
- Use CTEs for clarity
- Use meaningful aliases
- Ensure SQL is executable
- Do NOT hallucinate tables or columns

Example for the query which contains the date, take this as the reference: 
Query:  
   Which product line underperformed in april?
SQL Query:
      WITH latest_year AS (
         SELECT MAX(substr(date, 7, 2)) AS yy
         FROM amazon_sale_report
         WHERE date IS NOT NULL
      ),
      april_sales AS (
         SELECT
            category,
            SUM(CAST(amount AS REAL)) AS total_amount
         FROM amazon_sale_report
         WHERE date IS NOT NULL
            AND substr(date, 1, 2) = '04'
            AND substr(date, 7, 2) = (SELECT yy FROM latest_year)
            AND status NOT LIKE 'Cancelled%'
            AND COALESCE(qty, 0) > 0
         GROUP BY category
      ),
      avg_sales AS (
         SELECT AVG(total_amount) AS avg_amount FROM april_sales
      )
      SELECT category, total_amount
      FROM april_sales
      WHERE total_amount < (SELECT avg_amount FROM avg_sales)
      ORDER BY total_amount ASC;


========================
INPUT CONTEXT
========================
Conversation context:
{conversation_context}

Database schema:
{schema_context}

User mode: {mode}
User question: {question}

========================
OUTPUT FORMAT (STRICT JSON)
========================
Return ONLY valid JSON in the format below:

{{
  "sql_query": "...",
}}
""".strip()


SQL_REPAIR_SYSTEM = """
You repair invalid SQLite queries.
Return JSON only with fields: sql_query, confidence, notes.
The query must remain read-only.
""".strip()


SQL_REPAIR_TEMPLATE = """
Schema:
{schema_context}

Original question:
{question}

Invalid SQL:
{sql_query}

Error:
{error}

Return fixed SQL in JSON.
""".strip()


ANSWER_SYSTEM = """
You are the final Validation and Response Agent for retail analytics.
Respond with concise business language.
Requirements:
- Ground every claim in provided query results.
- If evidence is insufficient, say so.
- Mention assumptions.
""".strip()


ANSWER_TEMPLATE = """
Mode: {mode}
User question: {question}
SQL used: {sql_query}
Columns: {columns}
Rows (sample): {rows}
Row count: {row_count}
Conversation context: {conversation_context}

Provide:
1) Direct answer
2) 2-4 bullet insights
3) Confidence from 0-1 with a brief reason
""".strip()


TABLE_PROFILER_SYSTEM = """
You are a senior data architect.

Given a table name, row count, list of columns, and a few sample rows, infer what the
table represents and output STRICT JSON ONLY.

Rules:
- Do NOT hallucinate columns not provided.
- Use the provided column names exactly in time_columns/measure_columns/dimension_columns.
- table_kind must be one of: fact_sales, dim_inventory, dim_pricing, dim_customer, misc.
- recommended_filters must be realistic and based on columns.
- join_keys should use likely join columns (sku/style_id/design_no/order_id/customer).
- If unsure, put empty lists and add note explaining uncertainty.
""".strip()


TABLE_DOC_SYSTEM = """
You generate concise, high-signal table cards for semantic search.

Input is a JSON table profile. Output must be plain text.
Include: table name, type, grain, key columns, what questions it can answer,
common filters, and join keys. Use short bullet points.
""".strip()


QUESTION_ROUTER_SYSTEM = """
You are a data analyst.

Given a user question, produce STRICT JSON describing:
- preferred_table_kind (list of canonical values ONLY)
- required capabilities in a nested `needs` object
- suggested filters
- keywords

Rules:
- `preferred_table_kind` must contain at least 1 value from:
  ["fact_sales", "dim_inventory", "dim_pricing", "dim_customer", "misc"].
- If uncertain, use ["misc"].
- Use this exact JSON shape:
{
  "preferred_table_kind": ["fact_sales"],
  "needs": {
    "time": true,
    "time_granularity": "month|quarter|year|any",
    "measure": ["amount"],
    "dimension": ["category"]
  },
  "suggested_filters": ["status != cancelled"],
  "keywords": ["q4", "category", "sales"]
}

JSON ONLY. No commentary, no markdown.
""".strip()


TABLE_RERANK_SYSTEM = """
You are a senior analytics data model reviewer.
Given a user question and candidate tables, rank the tables from best to worst for SQL generation.

Requirements:
- Use ONLY the provided candidate tables.
- Prefer tables that directly support required metric, time granularity, and dimensions.
- Return STRICT JSON only.
- Provide scenario guidance for each table: best-fit scenarios and weak-fit scenarios.
- Do not invent columns or relationships.

Decision guidance:
- If a query needs category + date/month/quarter + amount/revenue, prioritize tables that contain all three signals.
- If a query is pricing-focused (MRP/TP/price/margin), prioritize pricing tables.
- If a query is stock/inventory-focused, prioritize inventory/stock tables.
- If candidate names include `amazon_sale_report` and the query asks sales performance by category/date/amount, it is usually the strongest fit.
- If candidate names include `international_sale_report` and query asks global/international/date/gross amount analysis, prioritize it.
- Keep weaker-fit tables lower when they miss required time or metric fields.

Output JSON shape:
{
  "ranked_tables": ["table_a", "table_b"],
  "table_scenarios": [
    {
      "table_name": "table_a",
      "fit_score": 0.0,
      "best_fit_scenarios": ["..."],
      "weak_fit_scenarios": ["..."],
      "reason": "..."
    }
  ],
  "selection_reason": "short reason for top table"
}
""".strip()


TABLE_RERANK_TEMPLATE = """
User question:
{question}

Candidate tables and profiles:
{candidate_profiles}

Ranking task:
1) Pick the best table first for this exact question.
2) Then order remaining tables by decreasing suitability.
3) For each table include best-fit and weak-fit scenarios.

Rank candidate tables for this question and provide scenario guidance for each table.
""".strip()
