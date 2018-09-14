
This is a Automated Tranzaction Software made in Python3 and MySQL.
It runs on a Rasberry Pi2 on a Ubuntu Mate 16 distribution.
It runs with several crons behind each starting the scripts.

When I was taking the decision to use cron I also considered to use
service (daemons) as processes but because of the memory limitations 
I prefered the cron way.
Sensitive data & trading strategy files are not present.


I will briefly explain what it does and how it works.

The software flow is the following :
- Interaction and mining part with the Bittrex REST API ;
- Insertion, updates, delete of records in DB
- Market analysis, decision making and action.

The structure (architecture) is the following :
Folders :
- conf folder : contains all the sensitive data such as accounts as passwords, logging and other configuration settings( sensitive data are not revealed) 
- includes folder : contains all functions and classes needed for the main run scripts ;
- img folder : contains plots of markets generated with the plot_graphs.py
- log folder : logs for each major operation
- tmp folder : temporary folder


Description :
- get_API_bittrex_data.py : interogates the REST API of bittrex and takes all the market data ;
It makes multiple operations, insertions in the tables of the DB, updates and also deletes.
When needed it also creates new tables.
runs every 10 minutes with cron

- avaialable_markets.py : This assures that all the markets are fully avaiable and when
there are markets which are pending delete on the bitrex side, is notifying the 
tmp/avaiable.markets.tmp and data is not taken from those markets anymore.
Also when the backup is made those markets ( tables in DB) will be deleted.

- plot_graphs.py : plots graphs using different kind of metrics.

- get_API_total_cmc.py : interogates the coinmarketcap.com and is similar with 
get_API_bittrex_data.py

- clean_and_backup_database.py : the cleanup tool and backup tool. It runs once per months
and deleted the records older than 1 months immediately after backuping them to another server.

- trade_agent.py : the trading agent does all the automated traiding and uses all avaiable data
from the markets in the database. Here it is empty because I don't want to reveal my trading
strategy.


NOTES : By using the DB_functions.py one can create all the database structure needed for this
app to run. For example when a new market appears, it automatically sees the market at runtime of
get_API_bittrex_data.py and creates the corresponding table.






