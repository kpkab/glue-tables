from pydantic import BaseModel, Field
from typing import Union, Optional, List

class Column(BaseModel):
    Name:str
    Type: Optional[str]
    Comment: Optional[str]
    Parameters: Optional[dict]

class Glue_table(BaseModel):
    Name : str = Field(min_length= 1, max_length= 255)
    DatabaseName: str = Field(min_length= 1, max_length= 255)
    Location: Optional[str]
    # Columns : List[Column]
    
class SuccessResponse(BaseModel):
    success: bool = True
    status:int = 200
    data: Union[None, dict, list] = None

class ErrorResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Something wrong"}

class ExceptionResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Unhandled Exception"}