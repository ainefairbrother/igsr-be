from pydantic import BaseModel

class Sample(BaseModel):
    id: str
    name: str | None = None
    population: str | None = None
    # Add/change fields to match your ES _source