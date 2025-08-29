from pydantic import BaseModel
from typing import Generic, List, TypeVar

T = TypeVar("T")

class Page(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int