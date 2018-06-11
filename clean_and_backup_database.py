#  Created by Bogdan Trif on 11-04-2018 , 3:00 PM.

import os
import operator
import pymysql
import requests  # to make GET request
from conf.db_conn import *
from includes.DB_functions import *
from includes.app_functions import *


connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
cur = connection.cursor()

### Special Folders ###
backup_date_dir = time.strftime("%Y-%m-%d",   time.localtime() )
bittrex_backup = '/mnt/vega_backup/Bittrex/DB/'+str(backup_date_dir)+'/bittrex'
coinmarketcap_backup = '/mnt/vega_backup/Bittrex/DB/'+str(backup_date_dir)+'/coinmarketcap'


### Preparation QUERIES   ####
#   BITTREX
prep_showTables = "SHOW TABLES FROM `bittrex` WHERE `Tables_in_bittrex` LIKE '%-%';"
prep_delete_1Month_older_rows = 'DELETE FROM `market_name` WHERE date < DATE_SUB(NOW(), INTERVAL 1 MONTH);'
prep_DROP_expired_market = 'DROP TABLE `market_name` ;'

#   COINMARKETCAP
prep_CMC_show_Tables = "SHOW TABLES FROM `coinmarketcap`;"


### Get the valid Markets from file :

valid_MARKETS = read_file_line_by_line( valid_markets_file )
print('valid_MARKETS : ', len(valid_MARKETS), valid_MARKETS )


print('prep_showTables : ', prep_showTables)
bittrex_markets_tables = cur.execute( prep_showTables )
# print('bittrex_markets_tables : ', bittrex_markets_tables)

markets = cur.fetchall()
now_DB_Markets = sorted([ i[0] for i in markets ])
file_append_with_text(DB_backup_log, Now_t(time.time())+ '   now_DB_Markets = ' +str(len(now_DB_Markets)) + '\n   ' + str(now_DB_Markets) )

print('now_DB_Markets : ',  len(now_DB_Markets), now_DB_Markets )

markets_to_DROP  = set(now_DB_Markets) - set( valid_MARKETS )
print('\nmarkets_to_DROP : ',  len(markets_to_DROP), markets_to_DROP )
file_append_with_text(DB_backup_log, Now_t(time.time())+ '   markets_to_DROP : ' +str(len(markets_to_DROP))+'\n   ' + str(markets_to_DROP) )

####        DUMP ALL THE TABLES     ####

#     Before all, we must mount the /mnt/vega_backup and create BACKUP DATE DIR
os.system("mount -t cifs //192.168.1.2/BACKUP /mnt/vega_backup -o user="+str(mnt_user)+",password="+str(mnt_passwd)+"")
if not os.path.exists(bittrex_backup): os.makedirs(bittrex_backup)


### Logs
file_append_with_text(DB_backup_log, Now_t(time.time())+ '  ########       START  BITTREX  BACKUP      ########' )


for cnt, market in enumerate(now_DB_Markets) :
    print(str(cnt+1)+'.    ', market)
    os.environ["market"] = market
    os.environ["market_backup_file"] = bittrex_backup+'/'+str(market)+'.sql'

    ### Before Dump we delete the records OLDER than 1 month, we do NOT want double data backup :
    try:
        # Execute the SQL command
        delete_old_records = cur.execute( prep_delete_1Month_older_rows.replace('market_name', market) )
        connection.commit()
    except :
        # Rollback in case there is any error
        connection.rollback()

    ### Logs
    file_append_with_text(DB_backup_log, Now_t(time.time()) + '   '+ str(cnt+1) + '   market : ' + str(market) +'  records older than 1 Month DELETED' )
    time.sleep(20)

    ######         ACTUAL MYSQL DUMP       ######

    ### WITHOUT Schema :
    # os.system("mysqldump -t -u default_username -pdefault_password bittrex $market --single-transaction --quick > $market_backup_file")
    os.system("mysqldump -t -u "+str(USER)+" -p"+ str(PASSWD)+" bittrex $market --single-transaction --quick > $market_backup_file")


    ### WITH Schema, used only the FIRST TIME :
    os.system("mysqldump -u "+str(USER)+" -p"+ str(PASSWD)+" bittrex $market --single-transaction --quick > $market_backup_file")

    ### Logs
    file_append_with_text(DB_backup_log, Now_t(time.time()) +  '   '+ str(cnt+1) + '   market : ' + str(market) +'  last Month was back-uped' )

    # if cnt == 2 : break
    if market in markets_to_DROP :
        cur.execute(prep_DROP_expired_market.replace('market_name', market) )

        ### Logs
        file_append_with_text(DB_backup_log, Now_t(time.time()) + '   '+ str(cnt+1) +  '   market : ' + str(market) +'    DROPPED !' )

    time.sleep(40)

file_append_with_text(DB_backup_log, Now_t(time.time()) + '  ########       END  BITTREX  BACKUP      ########' )






connection.commit()
connection.close()


## At the end umount /mnt/vega_backup
os.system("umount /mnt/vega_backup")