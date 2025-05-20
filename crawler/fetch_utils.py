#!/usr/bin/env python3
"""
fetch utils for stock data

"""

import os
import logging
import pandas as pd
import yfinance as yf
import datetime
from typing import Optional, Dict, Any

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

def load_stock_symbols(symbols_file: str) -> list:
    """
    Tải các mã cổ phiếu từ file CSV

    Tham số:
        symbols_file (str): Đường dẫn đến file CSV chứa các mã cổ phiếu

    Trả về:
        list: Danh sách các mã cổ phiếu
    """
    try:
        if not os.path.exists(symbols_file):
            logger.warning(f"Symbols file {symbols_file} not found, using default symbols")
            return ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA']
        
        df = pd.read_csv(symbols_file)
        symbols = df['Symbol'].tolist()
        logger.info(f"Loaded {len(symbols)} symbols from {symbols_file}")
        return symbols
    except Exception as e:
        logger.error(f"Error loading symbols from {symbols_file}: {e}")
        return ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA']
