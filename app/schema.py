from pydantic import BaseModel
from datetime import datetime

class SearchQuery(BaseModel):
    query: str 


class GameSchema(BaseModel):
    id: int
    name: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True