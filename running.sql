Select * from daily_price WHERE ticker = 'MYTE' AND Date >= '2022-06-16';

Delete from daily_price WHERE ticker = 'MYTE';

select * from period_returns WHERE return_date = '2009-03-30' AND monthly_return < 0 ORDER BY monthly_return DESC LIMIT 10;

Select * from top_bottom_deciles WHERE return_date = '2009-03-30' ORDER BY monthly_return ASC LIMIT 10;

select * from trade_portfolio;

select * from commodity_prices;

SELECT * FROM portfolio_value WHERE price_date = '2018-04-02';

select * from portfolio_performance WHERE value_date >= '2013-03-30' AND value_date <= '2023-03-27';

select * from sp100 WHERE price_date <= '2013-04-02';

ALTER TABLE portfolio_value CHANGE `past_price` `entry_price` decimal(13,6);

ALTER TABLE portfolio_performance MODIFY COLUMN benchmark_value decimal(13,6);

ALTER TABLE portfolio_performance
	ADD benchmark_units decimal(11,4) DEFAULT NULL AFTER returns_pct;

ALTER TABLE fin_ratios
	ADD
		(pcf_ratio decimal(11,6) DEFAULT NULL,
        ev_ebitda decimal(11,6) DEFAULT NULL,
		rd_revenue decimal(11,6) DEFAULT NULL,
        div_yield decimal(11,6) DEFAULT NULL);

Truncate trade_portfolio;

Truncate top_bottom_deciles;

Truncate period_returns;

Truncate portfolio_value;

Truncate portfolio_performance;

Truncate sp100;

Truncate daily_price;

ALTER TABLE period_return MODIFY ticker_id TINYTEXT;

SHOW processlist;
show status like '%onn%';
show status where variable_name = 'threads_connected';
show engine innodb status;

select * from sp100 where price_date = ;

select * from security;

SELECT * FROM latest_allocation;

select * from sentiment;

select * from action_funds WHERE mkt_price < txn_price;

select * from period_returns ORDER BY qtr_return ASC LIMIT 10;

select * from period_returns WHERE short_return > 1 AND long_return > 1 AND semi_annual_return > 1 ORDER BY semi_annual_return DESC;

select * from period_returns WHERE short_return > 0.8 AND semi_annual_return > 1.1 AND annual_return > 1.4 ORDER BY short_return ASC;

select * from period_returns ORDER BY annual_return DESC LIMIT 150;

Select * FROM z_scores ORDER BY weighted_z_score DESC LIMIT 150;

SELECT pr.ticker, pr.price, pr.short_return, pr.long_return, pr.semi_annual_return, pr.annual_return, zc.weighted_z_score 
	FROM period_returns AS pr
	JOIN z_scores AS zc ON zc.ticker = pr.ticker
    ORDER BY zc.weighted_z_score;

select COUNT(*) from period_returns;

TRUNCATE trade_portfolio;

DELETE FROM trade_portfolio;

TRUNCATE period_returns;

Truncate daily_price;

Truncate sp100;

ALTER TABLE period_returns CHANGE `long_return` `q_return` decimal(11,6) DEFAULT NULL;


ALTER TABLE daily_price CHANGE `Open` `Open` decimal(31,6);
ALTER TABLE daily_price CHANGE `High` `High` decimal(31,6);
ALTER TABLE daily_price CHANGE `Low` `Low` decimal(31,6);
ALTER TABLE daily_price CHANGE `Close` `Close` decimal(31,6);
ALTER TABLE daily_price CHANGE `Adj_Close` `Adj_Close` decimal(31,6);
ALTER TABLE daily_price CHANGE `Slope_Slow` `Slope_Slow` decimal(31,6);
ALTER TABLE daily_price CHANGE `Slope_Medium` `Slope_Medium` decimal(31,6);
ALTER TABLE daily_price CHANGE `Slope_Fast` `Slope_Fast` decimal(31,6);
ALTER TABLE daily_price CHANGE `PFE` `PFE` decimal(31,6);
ALTER TABLE daily_price CHANGE `RSI` `RSI` decimal(31,6);
ALTER TABLE daily_price CHANGE `EMA200` `EMA200` decimal(31,6);
ALTER TABLE daily_price CHANGE `EMA50` `EMA50` decimal(31,6);
ALTER TABLE daily_price CHANGE `EMA21` `EMA21` decimal(31,6);
ALTER TABLE daily_price CHANGE `RS` `RS` decimal(31,6);
ALTER TABLE daily_price CHANGE `Daily_Return` `Daily_Return` decimal(31,6);
ALTER TABLE daily_price CHANGE `Monthly_Return` `Monthly_Return` decimal(31,6);

ALTER TABLE action_funds MODIFY last_updated date AFTER created_date;

ALTER TABLE fin_ratios CHANGE `pe_ratio` `pe_ratio` decimal(11,6) DEFAULT NULL;

ALTER TABLE fin_ratios
	ADD
		(pcf_ratio decimal(11,6) DEFAULT NULL,
        ev_ebitda decimal(11,6) DEFAULT NULL,
		rd_revenue decimal(11,6) DEFAULT NULL,
        div_yield decimal(11,6) DEFAULT NULL);

ALTER TABLE daily_price
	ADD
		(net_profit_margin decimal(11,6) DEFAULT NULL);

ALTER TABLE daily_price
	ADD
		(net_profit_margin decimal(11,6) DEFAULT NULL,
        ps_ratio decimal(11,6) DEFAULT NULL,
		pb_ratio decimal(11,6) DEFAULT NULL,
        pe_ratio decimal(11,6) DEFAULT NULL);

ALTER TABLE fin_ratios
	ADD
		(ticker varchar(10) DEFAULT NULL);

ALTER TABLE fin_ratios MODIFY ticker varchar(10) AFTER id;
        
ALTER TABLE commodity_prices CHANGE `symbol` `symbol` varchar(40);

ALTER TABLE price_fundamentals CHANGE `pb_ratio` `pb_ratio` decimal(15,2);

Select * from z_scores;

DELETE FROM sp100;

INSERT INTO sp100 (price_date) 
	VALUES(2007-04-03 00:00:00);

TRUNCATE fin_ratios;

TRUNCATE z_scores;

Explain Select * from daily_price WHERE ticker = 'AAPL' AND Date >='2006-02-01' AND Date <= '2015-03-30';

Create index id ON daily_price(id);

SELECT * FROM `daily_price` WHERE ticker = 'AMZN' AND Date >= '2019-02-01';

SELECT DISTINCT ticker FROM `daily_price` WHERE ticker LIKE '%-%';

SET SQL_SAFE_UPDATES = 0;

SELECT COUNT(DISTINCT ticker) AS tic FROM daily_price;
SELECT COUNT(*) AS tot FROM daily_price;




    

11:32:33	DELETE FROM `daily_price` WHERE ticker = 'AAPL'	Error Code: 1175. You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.  To disable safe mode, toggle the option in Preferences -> SQL Editor and reconnect.	0.00042 sec

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

Select * from fin_ratios WHERE calc_date >= '2019-02-01' AND calc_date <= '2020-02-03' AND ticker = 'VIAC';

Select * from fin_ratios WHERE ticker = 'AAPL' AND calc_date > '2012-04-12';

Select * from fin_ratios;

UPDATE fin_ratios SET ticker = 'BRK-B' WHERE ticker = 'BRK.B';

TRUNCATE fin_ratios;


SELECT d.Close
    FROM daily_price AS d
    WHERE ticker = 'AAPL'
    LIMIT 1;

SELECT * from price_fundamentals WHERE ticker = 'AAPL' AND price_date >= '2010-01-01';


ALTER TABLE daily_price
ADD FOREIGN KEY (ticker) REFERENCES fin_ratios(ticker);



SELECT d.ID,
    d.Date AS Result_Date,
    d.pe_ratio,
    qt.pe_ratio AS Most_Recent_Quarterly_Values
FROM daily_price AS d
CROSS APPLY (
    SELECT q.pe_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = d.ticker
	AND q.calc_date <= d.Date
    LIMIT 1
    ORDER BY q.calc_date DESC
) qt



SELECT TOP 1 pe_ratio
    FROM fin_ratios AS q;
    

SELECT d.ticker,
    d.Date,
    d.pe_ratio,
    qt.pe_ratio 
FROM daily_price AS d
INNER JOIN fin_ratios ON fin_ratios.ticker =
    (SELECT q.ticker, q.pe_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = d.ticker
	AND q.calc_date <= d.Date
    LIMIT 1
    ORDER BY q.calc_date DESC
) qt

SELECT d.ticker,
    d.Date,
    d.Close,
    d.Adj_Close,
    (SELECT q.pe_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = 'MMM' AND d.ticker = 'MMM'
	AND q.calc_date <= d.Date
    ORDER BY q.calc_date DESC
    LIMIT 1) AS pe_ratio
    FROM daily_price AS d
    
INSERT INTO price_fundamentals (ticker, price_date, close_price, adj_close_price, ps_ratio, pb_ratio, pe_ratio) 
SELECT d.ticker,
    d.Date,
    d.Close,
    d.Adj_Close,
	(SELECT q.ps_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = d.ticker
	AND q.calc_date <= d.Date
    ORDER BY q.calc_date DESC
    LIMIT 1),
	(SELECT q.pb_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = d.ticker
	AND q.calc_date <= d.Date
    ORDER BY q.calc_date DESC
    LIMIT 1),
    (SELECT q.pe_ratio
    FROM fin_ratios AS q
    WHERE q.ticker = d.ticker
	AND q.calc_date <= d.Date
    ORDER BY q.calc_date DESC
    LIMIT 1)
    FROM daily_price AS d
    ;


    
    
set local innodb_parallel_read_threads=DEFAULT;
    
    
SELECT * FROM price_fundamentals WHERE ticker = 'AAPL' AND price_date >= '2014-06-03' AND price_date <='2022-04-12';

DELETE FROM price_fundamentals WHERE ticker = 'BF-B' and id > 1;

SELECT COUNT(*) FROM price_fundamentals;

TRUNCATE price_fundamentals;

TRUNCATE model_results;

TRUNCATE regression_returns;

Select * from daily_price where ticker = 'BRK-B' and Date >= '2009-01-01';

DELETE from price_fundamentals where pb_ratio is null;

DELETE from price_fundamentals where id > '1';

SELECT COUNT(*) FROM model_results WHERE Coeff_of_det > '0.5' AND Coeff_of_det < '0.85';

SELECT * FROM model_results WHERE ticker = 'CPB';

SELECT * FROM model_results WHERE Coeff_of_det > '0.1' AND Coeff_of_det < '0.92' AND slope_2 > '-3' AND pred_1 > '-1';

Select * from price_fundamentals WHERE ticker = 'MMM' AND price_date >= '2018-03-01';

DELETE FROM price_fundamentals WHERE ticker = 'MMM' AND created_date = '2021-06-22';

SELECT model_results.ticker, model_results.Coeff_of_det AS R_Squared, model_results.intercept, model_results.slope_1 AS Beta_1, model_results.slope_2 AS Beta_2, model_results.pred_1 AS predicted_price, price_fundamentals.close_price AS future_price
FROM model_results
INNER JOIN price_fundamentals
ON model_results.ticker = price_fundamentals.ticker
AND price_fundamentals.price_date = '2019-07-26';







