#!/usr/bin/env python3
"""
fetch utils for stock data

"""

import os
import logging
import pandas as pd
import yfinance as yf
import datetime
import requests
import tempfile
import csv
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Thiết lập logging
logger = logging.getLogger("fetch_utils")

def fetch_stock_history(ticker_symbol: str, period: str = "30d") -> Optional[pd.DataFrame]:
    """
    Lấy dữ liệu giá lịch sử cho một mã cổ phiếu cụ thể

    Tham số:
        ticker_symbol (str): Mã cổ phiếu (ví dụ: 'AAPL')
        period (str): Khoảng thời gian cần lấy (ví dụ: '30d', '1y', 'max')

    Trả về:
        Optional[pd.DataFrame]: DataFrame với dữ liệu lịch sử hoặc None nếu có lỗi
    """
    try:
        # Lấy ngày hiện tại và ngày cách đây một khoảng thời gian
        end_date = datetime.datetime.today()
        
        if period == "30d":
            start_date = end_date - datetime.timedelta(days=30)
        elif period == "max":
            start_date = None
        else:
            # Mặc định là 30 ngày nếu khoảng thời gian không xác định
            start_date = end_date - datetime.timedelta(days=30)
            
        # Tải dữ liệu cổ phiếu
        logger.info(f"Fetching history data for {ticker_symbol} for period {period}")
        
        df = yf.download(
            ticker_symbol,
            start=start_date.strftime('%Y-%m-%d') if start_date else None,
            end=end_date.strftime('%Y-%m-%d'),
            auto_adjust=False,
            progress=False
        )
        
        if df.empty:
            logger.warning(f"No history data available for {ticker_symbol}")
            return None
        
        # Đặt lại chỉ mục để biến ngày thành một cột
        df.reset_index(inplace=True)
        
        # Thêm cột mã cổ phiếu
        df['ticker'] = ticker_symbol
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching history data for {ticker_symbol}: {e}")
        return None

def fetch_stock_actions(ticker_symbol: str) -> Optional[pd.DataFrame]:
    """
    Lấy dữ liệu cổ tức và chia tách cổ phiếu cho một mã cổ phiếu cụ thể

    Tham số:
        ticker_symbol (str): Mã cổ phiếu (ví dụ: 'AAPL')

    Trả về:
        Optional[pd.DataFrame]: DataFrame với dữ liệu hành động cổ phiếu hoặc None nếu có lỗi
    """
    try:
        logger.info(f"Fetching actions data for {ticker_symbol}")
        
        # Tạo đối tượng Ticker
        ticker = yf.Ticker(ticker_symbol)
        
        # Lấy các hành động (cổ tức và chia tách)
        df_actions = ticker.actions
        
        if df_actions is None or df_actions.empty:
            logger.warning(f"No actions data available for {ticker_symbol}")
            return None
        
        # Đặt lại chỉ mục để biến ngày thành một cột
        df_actions = df_actions.reset_index().rename(columns={"Date": "date"})
        
        # Thêm cột mã cổ phiếu
        df_actions['ticker'] = ticker_symbol
        
        return df_actions
        
    except Exception as e:
        logger.error(f"Error fetching actions data for {ticker_symbol}: {e}")
        return None

def fetch_stock_info(ticker_symbol: str) -> Optional[Dict[str, Any]]:
    """
    Lấy thông tin công ty cho một mã cổ phiếu cụ thể

    Tham số:
        ticker_symbol (str): Mã cổ phiếu (ví dụ: 'AAPL')

    Trả về:
        Optional[Dict[str, Any]]: Dictionary với thông tin công ty hoặc None nếu có lỗi
    """
    try:
        logger.info(f"Fetching company info for {ticker_symbol}")
        
        # Tạo đối tượng Ticker
        ticker = yf.Ticker(ticker_symbol)
        
        # Lấy thông tin công ty
        info = ticker.info
        
        if not info:
            logger.warning(f"No company info available for {ticker_symbol}")
            return None
            
        # Thêm mã cổ phiếu một cách rõ ràng
        info['ticker'] = ticker_symbol
        
        return info
        
    except Exception as e:
        logger.error(f"Error fetching company info for {ticker_symbol}: {e}")
        return None

def fetch_stock_financials(ticker_symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Lấy báo cáo tài chính cho một mã cổ phiếu cụ thể

    Tham số:
        ticker_symbol (str): Mã cổ phiếu (ví dụ: 'AAPL')

    Trả về:
        Optional[Dict[str, pd.DataFrame]]: Dictionary với dữ liệu tài chính hoặc None nếu có lỗi
    """
    try:
        logger.info(f"Fetching financials for {ticker_symbol}")
        
        # Tạo đối tượng Ticker
        ticker = yf.Ticker(ticker_symbol)
        
        # Lấy dữ liệu tài chính
        financials = {
            'income_statement': ticker.income_stmt,
            'balance_sheet': ticker.balance_sheet,
            'cash_flow': ticker.cashflow
        }
        
        # Kiểm tra xem chúng ta có nhận được dữ liệu nào không
        if all(df is None or df.empty for df in financials.values()):
            logger.warning(f"No financial data available for {ticker_symbol}")
            return None
            
        # Xử lý từng dataframe
        for key, df in financials.items():
            if df is not None and not df.empty:
                df['ticker'] = ticker_symbol
        
        return financials
        
    except Exception as e:
        logger.error(f"Error fetching financials for {ticker_symbol}: {e}")
        return None

def download_file_from_google_drive(file_id: str, destination: str) -> bool:
    """
    Download a file from Google Drive using the file ID
    
    Args:
        file_id (str): Google Drive file ID
        destination (str): Local path where the file should be saved
        
    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        # Google Drive download URL for public files
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        logger.info(f"Downloading file from Google Drive: {file_id}")
        
        # Create a session to handle redirects
        session = requests.Session()
        
        # First request to get the file
        response = session.get(download_url, stream=True)
        response.raise_for_status()
        
        # Check if we need to handle the virus scan warning
        if 'virus scan warning' in response.text.lower():
            # Extract the confirm token from the response
            for line in response.text.splitlines():
                if 'confirm=' in line:
                    confirm_token = line.split('confirm=')[1].split('&')[0].split('"')[0]
                    break
            else:
                confirm_token = None
            
            if confirm_token:
                # Make another request with the confirm token
                download_url = f"https://drive.google.com/uc?export=download&confirm={confirm_token}&id={file_id}"
                response = session.get(download_url, stream=True)
                response.raise_for_status()
        
        # Write the file to destination
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"Successfully downloaded file to {destination}")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading file from Google Drive: {e}")
        return False
    except Exception as e:
        logger.error(f"Error downloading file from Google Drive: {e}")
        return False

def load_stock_symbols_from_google_drive() -> List[str]:
    """
    Load stock symbols from Google Drive CSV file
    
    Returns:
        List[str]: List of stock symbols
    """
    try:
        # Get Google Drive configuration from environment variables
        file_id = os.getenv('GOOGLE_DRIVE_FILE_ID')
        
        if not file_id:
            logger.error("GOOGLE_DRIVE_FILE_ID not found in environment variables")
            return get_default_symbols()
        
        # Create a temporary file to store the downloaded CSV
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Download the file from Google Drive
            if not download_file_from_google_drive(file_id, temp_path):
                logger.error("Failed to download symbols file from Google Drive")
                return get_default_symbols()
            
            # Read the CSV file
            df = pd.read_csv(temp_path)
            
            # Extract symbols from the 'Symbol' column
            if 'Symbol' not in df.columns:
                logger.error("'Symbol' column not found in the downloaded CSV file")
                return get_default_symbols()
            
            symbols = df['Symbol'].dropna().astype(str).tolist()
            
            # Remove any empty strings or whitespace-only symbols
            symbols = [symbol.strip() for symbol in symbols if symbol.strip()]
            
            logger.info(f"Successfully loaded {len(symbols)} symbols from Google Drive")
            return symbols
            
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_path)
                logger.debug(f"Cleaned up temporary file: {temp_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary file {temp_path}: {cleanup_error}")
        
    except Exception as e:
        logger.error(f"Error loading symbols from Google Drive: {e}")
        return get_default_symbols()

def get_default_symbols() -> List[str]:
    """
    Get default stock symbols as fallback
    
    Returns:
        List[str]: List of default stock symbols
    """
    default_symbols = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM']
    logger.info(f"Using default symbols: {default_symbols}")
    return default_symbols

def load_stock_symbols(symbols_file: str = None) -> List[str]:
    """
    Load stock symbols from Google Drive (symbols_file parameter is kept for backward compatibility but ignored)
    
    Args:
        symbols_file (str, optional): Deprecated parameter, kept for backward compatibility
        
    Returns:
        List[str]: List of stock symbols
    """
    if symbols_file:
        logger.warning("symbols_file parameter is deprecated. Loading symbols from Google Drive instead.")
    
    return load_stock_symbols_from_google_drive()
