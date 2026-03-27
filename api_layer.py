# This file contains the FastAPI application that exposes endpoints
# for the frontend GUI to send SQL queries and receive analysis results.

from fastapi import FastAPI
from pydantic import BaseModel
from service_layer import process_query  # Import service layer pipeline

# Initialize FastAPI application
app = FastAPI()

# Define the expected input JSON from the frontend
class QueryRequest(BaseModel):
    """
    Pydantic model to validate input JSON.
    Expected JSON format from GUI:
    {
        "query": "SELECT * FROM table;"
    }
    """
    query: str  # SQL query string from the user

# Define the /analyze POST endpoint
@app.post("/analyze")
def analyze_query(request: QueryRequest):
    """
    API endpoint to analyze SQL queries.
    Steps:
    1. Extract SQL query from request
    2. Call the service layer pipeline (process_query)
    3. Return the result JSON back to the frontend
    """
    query = request.query          # Extract SQL query
    result = process_query(query)  # Process query through service layer
    return result                  # Return JSON response