from pydantic import BaseModel, Field, field_validator
from typing import Optional
from fractions import Fraction

class SecExtract(BaseModel):
    symbol: str = Field(description="Stock Ticker Symbol")
    filing_period: str = Field(description="quarter end date (in YYYY-MM-DD format)")
    eps_basic: str = Field(description="basic earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_diluted: str = Field(description="diluted earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_basic9: Optional[str] = Field(default=None, description="basic earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")
    eps_diluted9: Optional[str] = Field(default=None, description="diluted earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")

class VisaExtract(BaseModel):
    symbol: str = Field(description="Stock Ticker Symbol")
    filing_period: str = Field(description="quarter end date (in YYYY-MM-DD format)")
    eps_basic_classA: str = Field(description="Class A basic earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_diluted_classA: str = Field(description="Class A diluted earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_basic9_classA: Optional[str] = Field(default=None, description="Class A basic earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")
    eps_diluted9_classA: Optional[str] = Field(default=None, description="Class A diluted earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")

class GoogleExtract(BaseModel):
    symbol: str = Field(description="Stock Ticker Symbol")
    filing_period: str = Field(description="quarter end date (in YYYY-MM-DD format)")
    eps_basic_classA_GOOGL: str = Field(description="Class A GOOGL basic earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_diluted_classA_GOOGL: str = Field(description="Class A GOOGL diluted earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_basic9_classA_GOOGL: Optional[str] = Field(default=None, description="Class A GOOGL basic earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")
    eps_diluted9_classA_GOOGL: Optional[str] = Field(default=None, description="Class A GOOGL diluted earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")

    eps_basic_classC_GOOG: str = Field(description="Class C GOOG basic earnings (net income) per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_diluted_classC_GOOG: str = Field(description="Class C GOOG diluted earning (net income)s per share, 3 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number.")
    eps_basic9_classC_GOOG: Optional[str] = Field(default=None, description="Class C GOOG basic earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")
    eps_diluted9_classC_GOOG: Optional[str] = Field(default=None, description="Class C GOOG diluted earnings (net income) per share, 9 months ended current filing year. If wrapped in a bracket, prepend a negative sign to the number. If field cannot be found, do not return anything.")

class BACExtract(BaseModel):
    symbol: str = Field(description="Stock Ticker Symbol")
    filing_period: str = Field(description="quarter end date (in YYYY-MM-DD format)")
    eps_basic: str = Field(description="basic earnings per share, 3 months ended current filing year")
    eps_diluted: str = Field(description="diluted earnings per share, 3 months ended current filing year")
    eps_basic9: Optional[str] = Field(default=None, description="basic earnings per share, 9 months ended current filing year")
    eps_diluted9: Optional[str] = Field(default=None, description="diluted earnings per share, 9 months ended current filing year")

    dividend_rate: str = Field(description="Dividend rate as a percent or decimal (e.g., '6%' or '0.06')")
    par_value: float = Field(description="Par value of each preferred share, e.g., 1000")
    depositary_ratio: str = Field(description="Fraction string representing depositary share ratio (e.g., '1/1000')")

    dps: Optional[float] = Field(default=None, description="Calculated dividend per depositary share")

    @field_validator("dividend_rate", mode="before")
    def parse_dividend_rate(cls, v):
        if isinstance(v, str):
            return float(v.strip('%')) / 100 if '%' in v else float(v)
        return v

    @field_validator("depositary_ratio", mode="before")
    def parse_depositary_ratio(cls, v):
        if isinstance(v, str):
            return float(Fraction(v))
        return float(v)

    @field_validator("dps", mode="after")
    def compute_dps(cls, v, values):
        rate = values.get("dividend_rate")
        par = values.get("par_value")
        ratio = values.get("depositary_ratio")
        return round(par * rate * ratio, 6) if rate and par and ratio else None 
