-- Author: Vikas Lamba
-- Date: 2025-08-14
-- Description: This script creates the database schema for the Leo Trading Platform.

CREATE DATABASE IF NOT EXISTS algotrader;

USE algotrader;

-- Table to store information about the stocks
CREATE TABLE IF NOT EXISTS stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255),
    exchange VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table to store daily stock prices
CREATE TABLE IF NOT EXISTS stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    price_date DATE NOT NULL,
    open_price DECIMAL(19, 4),
    high_price DECIMAL(19, 4),
    low_price DECIMAL(19, 4),
    close_price DECIMAL(19, 4),
    adj_close_price DECIMAL(19, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `idx_stock_date` (`stock_id`, `price_date`),
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table to store calculated technical indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_price_id INT NOT NULL,
    ema_21 DECIMAL(19, 4),
    ema_50 DECIMAL(19, 4),
    ema_200 DECIMAL(19, 4),
    rsi DECIMAL(19, 4),
    macd DECIMAL(19, 4),
    signal_line DECIMAL(19, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_price_id) REFERENCES stock_prices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Add other tables as per the initial request for future use, but without the complexity of the original file.

-- Table for users
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for trade signals
CREATE TABLE IF NOT EXISTS signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    signal_date DATE NOT NULL,
    signal_type VARCHAR(10) NOT NULL, -- e.g., 'BUY', 'SELL', 'HOLD'
    confidence DECIMAL(5, 4),
    source VARCHAR(100), -- e.g., 'RSI_MODEL', 'MACD_CROSSOVER'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for paper trades
CREATE TABLE IF NOT EXISTS trades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    stock_id INT NOT NULL,
    trade_date DATETIME NOT NULL,
    trade_type VARCHAR(4) NOT NULL, -- 'BUY' or 'SELL'
    quantity INT NOT NULL,
    price DECIMAL(19, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for backtesting results
CREATE TABLE IF NOT EXISTS backtest_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    stock_id INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital DECIMAL(19, 4) NOT NULL,
    final_capital DECIMAL(19, 4) NOT NULL,
    total_return_pct DECIMAL(10, 4),
    sharpe_ratio DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for news feeds
CREATE TABLE IF NOT EXISTS news_feeds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT,
    news_date DATETIME NOT NULL,
    source VARCHAR(255),
    headline TEXT NOT NULL,
    url VARCHAR(255),
    sentiment VARCHAR(10), -- e.g., 'POSITIVE', 'NEGATIVE', 'NEUTRAL'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
