Work done so far
1) Schema for stock_ticks (with OHLC ingestion bash script - currently defaulted to symbol of 'AAPL')
2) Schema for article
3) Schema for article chunks ( set to embedding size of 384 corresponding to "all-MiniLM-L6-v2")
4) Schema for stock_metadata (currently manually inserting AAPL)
5) Hypertable between 'stock_ticks' and 'time'
6) Batch processing of workload 1 by cross product SYMBOL, WINDOW and RATIO - WINDOW sizes {1 week , 2 weeks} , RATIO {0.05, 0.10, 0.15}, SYMBOL - AAPL and save results to csv file


Pending
1) group by industry - need to figure out which json content is important and ingest into psql
2) expand to more symbols & automate ingestion of stock metadata
3) expand database to correspond to actual workload 1 task - overlapping 1-week or 2-week windows starting 1-Jan-2014 but currently starting from 1-Jan-2013