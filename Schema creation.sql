CREATE DATABASE Time_Series;

USE Time_Series;
CREATE TABLE `action_funds` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker` varchar(10) NOT NULL,
 `quantity` int(11) NOT NULL,
 `price_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `price` decimal(11,6) DEFAULT NULL,
 `value` decimal(11,6) DEFAULT NULL,
 `action` varchar(10) DEFAULT NULL,
 `funds_available` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`),
 KEY `ticker_id` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `daily_price` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `data_vendor_id` int(11) NOT NULL,
 `ticker` varchar(10) NOT NULL,
 `price_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `open_price` decimal(11,6) DEFAULT NULL,
 `high_price` decimal(11,6) DEFAULT NULL,
 `low_price` decimal(11,6) DEFAULT NULL,
 `close_price` decimal(11,6) DEFAULT NULL,
 `adj_close_price` decimal(11,6) DEFAULT NULL,
 `volume` bigint(20) DEFAULT NULL,
 `Return` decimal(11,6) DEFAULT NULL,
 `EMA21` decimal(11,6) DEFAULT NULL,
 `EMA50` decimal(11,6) DEFAULT NULL,
 `EMA200` decimal(11,6) DEFAULT NULL,
 `RS` decimal(11,6) DEFAULT NULL,
 `RSI` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

alter table daily_price drop primary key, add primary key(k1, k2, k3);

CREATE TABLE `latest_allocation` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker` varchar(10) NOT NULL,
 `allocation_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `allocation_date` (`allocation_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `period_returns` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker` varchar(10) NOT NULL,
 `return_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `return` decimal(11,6) DEFAULT NULL,
 `price` decimal(11,6) DEFAULT NULL,
 `sma_20` decimal(11,6) DEFAULT NULL,
 `sma_50` decimal(11,6) DEFAULT NULL,
 `sma_200` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `return_date` (`return_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `top_bottom_deciles` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `return_date` date NOT NULL,
 `ticker` varchar(10) NOT NULL,
 `decile` varchar(10) NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `price` decimal(11,6) DEFAULT NULL,
 `monthly_return` decimal(11,6) DEFAULT NULL,
 `quarterly_return` decimal(11,6) DEFAULT NULL,
 `semi_annual_return` decimal(11,6) DEFAULT NULL,
 `annual_return` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `return_date` (`return_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `commodity_prices` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `symbol` varchar(10) NOT NULL,
 `price_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `open_price` decimal(11,6) DEFAULT NULL,
 `high_price` decimal(11,6) DEFAULT NULL,
 `low_price` decimal(11,6) DEFAULT NULL,
 `close_price` decimal(11,6) DEFAULT NULL,
 `volume` bigint(20) DEFAULT NULL,
 `daily_return` decimal(11,6) DEFAULT NULL,
 `monthly_return` decimal(11,6) DEFAULT NULL,
 `qtr_return` decimal(11,6) DEFAULT NULL,
 `semi_annual_return` decimal(11,6) DEFAULT NULL,
 `annual_return` decimal(11,6) DEFAULT NULL,
 `EMA21` decimal(11,6) DEFAULT NULL,
 `EMA50` decimal(11,6) DEFAULT NULL,
 `EMA200` decimal(11,6) DEFAULT NULL,
 `RS` decimal(11,6) DEFAULT NULL,
 `RSI` decimal(11,6) DEFAULT NULL,
 `PFE` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `portfolio_value` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `price_date` date NOT NULL,
 `portfolio_value` decimal(11,6) NOT NULL,
 `portfolio_return` decimal(11,6) DEFAULT NULL,
 `total_return` decimal(11,6) DEFAULT NULL,
 `benchmark_value` decimal(11,6) NOT NULL,
 `total_portfolio_value` decimal(11,6) NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `portfolio_performance` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `position_date` date NOT NULL,
 `value_date` date NOT NULL,
 `portfolio_value` decimal(11,6) NOT NULL,
 `portfolio_return` decimal(11,6) DEFAULT NULL,
 `benchmark_value` decimal(11,6) NOT NULL,
 `benchmark_return` decimal(11,6) DEFAULT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `value_date` (`value_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `security` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `exchange_id` int(11) DEFAULT NULL,
 `ticker` varchar(10) NOT NULL,
 `name` varchar(100) DEFAULT NULL,
 `sector` varchar(100) DEFAULT NULL,
 `industry` varchar(100) DEFAULT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `exchange_id` (`exchange_id`),
 KEY `ticker` (`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `sentiment` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `sentiment_date` date NOT NULL,
 `bullish` decimal(11,6) NOT NULL,
 `neutral` decimal(11,6) DEFAULT NULL,
 `bearish` decimal(11,6) DEFAULT NULL,
 `total` decimal(11,6) DEFAULT NULL,
 `bullish-8-week-MA` decimal(11,6) NOT NULL,
 `bull-bear-spread` decimal(11,6) DEFAULT NULL,
 `bullish-average` decimal(11,6) DEFAULT NULL,
 `bullish-ave-plus-std` decimal(11,6) DEFAULT NULL,
 `bullish-ave-minus-std` decimal(11,6) DEFAULT NULL,
 `sp500-weekly-high` decimal(11,6) DEFAULT NULL,
 `sp500-weekly-low` decimal(11,6) DEFAULT NULL,
 `sp500-weekly-close` decimal(11,6) DEFAULT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `sentiment_date` (`sentiment_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `sp100` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker_id` int(11) DEFAULT NULL,
 `price_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `open_price` decimal(11,6) DEFAULT NULL,
 `high_price` decimal(11,6) DEFAULT NULL,
 `low_price` decimal(11,6) DEFAULT NULL,
 `close_price` decimal(11,6) DEFAULT NULL,
 `adj_close_price` decimal(11,6) DEFAULT NULL,
 `volume` bigint(20) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `top_selection` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker` varchar(10) NOT NULL,
 `selection_date` date NOT NULL,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `selection_date` (`selection_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `trade_portfolio` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `price_date` date NOT NULL,
 `ticker` varchar(11) NOT NULL,
 `quantity` decimal(11,6) NOT NULL,
 `price` decimal(11,6) DEFAULT NULL,
 `value` decimal(11,6) NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `price_fundamentals` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticker` varchar(10) NOT NULL,
 `price_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `close_price` decimal(11,6) DEFAULT NULL,
 `adj_close_price` decimal(11,6) DEFAULT NULL,
 `ps_ratio` decimal(11,6) DEFAULT NULL,
 `pb_ratio` decimal(11,6) DEFAULT NULL,
 `pe_ratio` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`),
 KEY `price_date` (`price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `fin_ratios` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `calc_date` date NOT NULL,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `net_profit_margin` decimal(11,6) DEFAULT NULL,
 `ps_ratio` decimal(11,6) DEFAULT NULL,
 `pb_ratio` decimal(11,6) DEFAULT NULL,
 `pe_ratio` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8


CREATE TABLE `model_results` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `model_name` varchar(24) NOT NULL,
 `ticker` varchar(10) NOT NULL,
 `Coeff_of_det` decimal(11,6) DEFAULT NULL,
 `intercept` decimal(11,6) DEFAULT NULL,
 `slope_1` decimal(11,6) DEFAULT NULL,
 `slope_2` decimal(11,6) DEFAULT NULL,
 `slope_3` decimal(11,6) DEFAULT NULL,
 `pred_1` decimal(11,6) DEFAULT NULL,
 `pred_2` decimal(11,6) DEFAULT NULL,
 `pred_3` decimal(11,6) DEFAULT NULL,
 `pred_4` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `regression_returns` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `calc_date` date NOT NULL,
 `reg_model_name` varchar(24) NOT NULL,
 `ticker` varchar(10) NOT NULL,
 `alpha` decimal(11,6) DEFAULT NULL,
 `ps_ratio` decimal(11,6) DEFAULT NULL,
 `pe_ratio` decimal(11,6) DEFAULT NULL,
 `pcf_ratio` decimal(11,6) DEFAULT NULL,
 `graham_number` decimal(11,6) DEFAULT NULL,
 `ev_ebitda` decimal(11,6) DEFAULT NULL,
 `rd_revenue` decimal(11,6) DEFAULT NULL,
 `volume` decimal(11,6) DEFAULT NULL,
 `rsi` decimal(11,6) DEFAULT NULL,
 `ema21` decimal(11,6) DEFAULT NULL,
 `ema50` decimal(11,6) DEFAULT NULL,
 `ema200` decimal(11,6) DEFAULT NULL,
 `pfe` decimal(11,6) DEFAULT NULL,
 `kvp1` decimal(11,6) DEFAULT NULL,
 `kvp2` decimal(11,6) DEFAULT NULL,
 `kvp3` decimal(11,6) DEFAULT NULL,
 `kvp4` decimal(11,6) DEFAULT NULL,
 `kvp5` decimal(11,6) DEFAULT NULL,
 `kvp6` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP table regression_returns;

CREATE TABLE `regression_returns` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `calc_date` date,
 `reg_model_name` varchar(24) NOT NULL,
 `ticker` varchar(10) NOT NULL,
 `factor` varchar(10) NOT NULL,
 `coeff` decimal(11,6) DEFAULT NULL,
 `std_error` decimal(11,6) DEFAULT NULL,
 `t_statistic` decimal(11,6) DEFAULT NULL,
 `p_value` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `z_scores` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
 `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `calc_date` date,
 `ticker` varchar(10) NOT NULL,
 `factor_name` varchar(10) NOT NULL,
 `factor_value` decimal(11,6) DEFAULT NULL,
 `univ_factor_mean` decimal(11,6) DEFAULT NULL,
 `weighted_z_score` decimal(11,6) DEFAULT NULL,
 `std_deviation` decimal(11,6) DEFAULT NULL,
 `factor_z_score` decimal(11,6) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE z_scores CHANGE `std_deviation` `std_deviation` decimal(17,6);

ALTER TABLE z_scores MODIFY Weighted_Z_Score decimal(11,6) AFTER z_score;
