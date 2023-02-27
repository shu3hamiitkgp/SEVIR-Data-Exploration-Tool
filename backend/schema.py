from pydantic import BaseModel
from typing import Union

class goes_url(BaseModel):
    station: str
    year: str
    day: str
    hour: str
    file: str

class goes_hour(BaseModel):
    station: str
    year: str
    day: str

class goes_file(BaseModel):
    station: str
    year: str
    day: str
    hour: str

class goes_day(BaseModel):
    station: str
    year: str

class goes_year(BaseModel):
    station: str

class Nexrad_S3_generate_url(BaseModel):
    target_bucket: str
    user_key: str

class Nexard_S3_upload_file(BaseModel):
    key: str
    source_bucket: str
    target_bucket: str

class Nexrad_S3_fetch_url(BaseModel):
    year: str
    month: str
    day: str
    station: str
    file:str

class Nexrad_S3_fetch_file(BaseModel):
    year: str
    month: str
    day: str
    station: str

class Nexrad_S3_fetch_station(BaseModel):
    year: str
    month: str
    day: str

class Nexrad_S3_fetch_day(BaseModel):
    year: str
    month: str

class Nexrad_S3_fetch_month(BaseModel):
    yearSelected: str

class ValidateFile(BaseModel):
    file_name: str

class LoginData(BaseModel):
    Username: str
    Password: str

class TokenClass(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None