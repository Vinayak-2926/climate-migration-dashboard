"""
Shared utility functions for both pipeline and web application contexts.

This module provides framework-agnostic utility functions that can be used
across different parts of the application without introducing dependencies
on specific frameworks like Streamlit.
"""

from pathlib import Path
from typing import Optional, Union
import pandas as pd


def validate_county_fips(county_fips: Union[str, int]) -> str:
    """
    Validate and format county FIPS code to ensure it's a 5-digit string.
    
    Parameters:
    -----------
    county_fips : str or int
        County FIPS code to validate
        
    Returns:
    --------
    str
        Formatted 5-digit FIPS code
        
    Raises:
    -------
    ValueError
        If FIPS code is invalid
    """
    if county_fips is None:
        raise ValueError("County FIPS code cannot be None")
    
    # Convert to string and remove any whitespace
    fips_str = str(county_fips).strip()
    
    # Remove any non-digit characters
    fips_digits = ''.join(c for c in fips_str if c.isdigit())
    
    # Validate length
    if len(fips_digits) == 0:
        raise ValueError("County FIPS code must contain digits")
    elif len(fips_digits) > 5:
        raise ValueError("County FIPS code cannot be longer than 5 digits")
    
    # Zero-pad to 5 digits
    return fips_digits.zfill(5)


def validate_file_path(file_path: Union[str, Path], must_exist: bool = True) -> Path:
    """
    Validate and convert file path to Path object.
    
    Parameters:
    -----------
    file_path : str or Path
        File path to validate
    must_exist : bool
        Whether the file must already exist
        
    Returns:
    --------
    Path
        Validated Path object
        
    Raises:
    -------
    FileNotFoundError
        If must_exist=True and file doesn't exist
    ValueError
        If path is invalid
    """
    if not file_path:
        raise ValueError("File path cannot be empty")
    
    path_obj = Path(file_path)
    
    if must_exist and not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_obj}")
    
    return path_obj


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default value if denominator is zero.
    
    Parameters:
    -----------
    numerator : float
        Numerator value
    denominator : float
        Denominator value
    default : float
        Value to return if denominator is zero
        
    Returns:
    --------
    float
        Result of division or default value
    """
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ValueError):
        return default


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column names by removing quotes and standardizing format.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with potentially messy column names
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with cleaned column names
    """
    df_copy = df.copy()
    
    # Remove quotes and extra whitespace from column names
    df_copy.columns = [
        col.strip().strip('"').strip("'") 
        for col in df_copy.columns
    ]
    
    return df_copy


def format_number(value: Optional[Union[int, float]], precision: int = 2, 
                 include_commas: bool = True) -> str:
    """
    Format a number for display with proper precision and comma separation.
    
    Parameters:
    -----------
    value : int, float, or None
        Number to format
    precision : int
        Number of decimal places
    include_commas : bool
        Whether to include comma separators
        
    Returns:
    --------
    str
        Formatted number string or "N/A" if value is None
    """
    if value is None or pd.isna(value):
        return "N/A"
    
    try:
        if include_commas:
            return f"{value:,.{precision}f}"
        else:
            return f"{value:.{precision}f}"
    except (ValueError, TypeError):
        return "N/A"


def ensure_directory_exists(directory_path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Parameters:
    -----------
    directory_path : str or Path
        Directory path to ensure exists
        
    Returns:
    --------
    Path
        Path object for the directory
    """
    path_obj = Path(directory_path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj