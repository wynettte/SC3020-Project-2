from fastapi import FastAPI
from pydantic import BaseModel
from project import process_query

# FastAPI application instance
app = FastAPI()

# To request schema for the incoming API calls
class QueryRequest(BaseModel):
    # User will be providing the SQL query
    query: str 

@app.post("/analyze")
def analyze(request: QueryRequest):
    # This endpoint will be receiving a SQL query from GUI, which passes it to the backend
    # and returns as a JSON for the analysis result
    return process_query(request.query)