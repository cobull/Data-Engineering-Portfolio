# Work In Progress

## Summary

This project is an ETL (Extract, Transform, Load) pipeline that extracts stock and ETF data from the TwelveData API <https://twelvedata.com/>, processes and transforms the data, and loads it into a PostgreSQL database for further analysis and reporting. Specifically, this pipeline extracts hourly exchange data (open, close, high, low, volume) and hourly, technical indicator data (Average Directional Index, Percent B, Relative Strength Index, and Exponential Moving Average) for each stock and etf. Data for the following stocks and ETFs are gathered: SPY, XOM, USDX, VIXY, GLD, QQQ, ARKK, and IBIT. Addtionally, a Power BI dashboard was created to display the hourly close, volume, and technical indicator values. Slicers were utilized in the dashboard to allow users to easily switch between tickers and technical indicators. A PDF snapshot of the report can be found in the dashboard.pdf file.

## Dependencies

Python 3.12.6
PostgreSQL 17
Psycopg2 2.9.10

A TwelveData API key is also needed. 



