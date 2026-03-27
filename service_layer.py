# This file contains the core service logic of the backend.
# It acts as a pipeline connecting the API layer to the processing logic.
# Currently, placeholder functions are used for QEP, AQP, and Annotation
# so that the API works even before teammates implement the real logic.

# Placeholder function for Query Execution Plan (QEP)
def get_qep(query):
    """
    [1] Query Plan Extractor
    Input: SQL query string
    Output: structured JSON representing the query execution plan
    Note: Currently a placeholder. Teammates will implement actual extraction from PostgreSQL.
    """
    return {"qep": "not implemented yet"}

# Placeholder function for Alternative Query Plans (AQP)
def get_aqp(query):
    """
    [2] AQP Generator
    Input: SQL query string
    Output: List of alternative execution plans
    Note: Currently a placeholder. Teammates will implement logic to generate AQP variations.
    """
    return [{"aqp": "not implemented yet"}]

# Placeholder function for Annotation Engine
def annotate_query(query, qep, aqp_list):
    """
    [3] Annotation Engine
    Input: SQL query, QEP, and list of AQPs
    Output: Annotated SQL query explaining execution
    Note: Currently a placeholder. Teammates will add logic to produce meaningful annotations.
    """
    return "Annotation not implemented yet"

# Main service function
def process_query(query):
    """
    Main pipeline of the backend.
    1. Get the Query Execution Plan (QEP)
    2. Generate Alternative Query Plans (AQPs)
    3. Annotate the SQL query using QEP and AQPs
    Returns a JSON object with all the results.
    """
    qep = get_qep(query)            # Call QEP extractor
    aqp_list = get_aqp(query)       # Call AQP generator
    annotated = annotate_query(query, qep, aqp_list)  # Call Annotation engine

    # Return all outputs in a single JSON object for the API
    return {
        "annotated_query": annotated,  # Annotated SQL string
        "qep": qep,                    # Original query execution plan
        "aqp": aqp_list                # Alternative plans
    }