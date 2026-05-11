#!/bin/bash

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
LOG_FILE="/opt/scripts/test/nike_moss_run.log"


if ! command -v ts &> /dev/null; then
   
    exec > >(while read line; do echo "$(date '+%Y-%m-%d %H:%M:%S') $line" >> "$LOG_FILE"; done) 2>&1
else
  
    exec > >(ts '[%Y-%m-%d %H:%M:%S]' >> "$LOG_FILE") 2>&1
fi

echo ">>> 脚本开始运行..."
error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D mongodb -e "

--   product表用来匹配   吊牌价   季节  大类  性别
truncate table erp_db.product_trans;
insert into erp_db.product_trans 
with product_article_no as (select replace(trim(article_no),' ','') as article_no from erp_db.product where article_no != '' and deleted = '0' group by replace(trim(article_no),' ','') ),
-- 品牌
product_brand    as (select article_no,brand,cn from (select *,row_number() over (partition by article_no order by cn desc) rn from (select article_no,brand,count(*) as cn from erp_db.product  where article_no != '' and deleted = '0' and brand is not null group by article_no,brand) as tab_brand01) as tab_brand02 where rn = 1),
-- 性别
product_sex      as (select article_no,sex,cn from (select *,row_number() over (partition by article_no order by cn desc) rn from (select article_no,sex,count(*) as cn from erp_db.product  where article_no != '' and deleted = '0' and sex is not null group by article_no,sex) as tab_brand01) as tab_brand02 where rn = 1),
-- 类别
product_category as (select article_no,category,cn from (select *,row_number() over (partition by article_no order by cn desc) rn from (select article_no,category,count(*) as cn from erp_db.product  where article_no != '' and deleted = '0' and category is not null group by article_no,category) as tab_brand01) as tab_brand02 where rn = 1),
-- tag_price
product_tagprice as (select article_no,tag_price,cn from (select *,row_number() over (partition by article_no order by cn desc) rn from (select article_no,tag_price,count(*) as cn from erp_db.product  where article_no != '' and deleted = '0' and tag_price is not null group by article_no,tag_price) as tab_brand01) as tab_brand02 where rn = 1),
-- 季节
product_season   as (select article_no,time_to_market,cn from (
select *,row_number() over (partition by article_no order by cn desc) rn from (
select article_no,time_to_market,count(*) as cn from erp_db.product_season where time_to_market like '%Q%'  and (length(time_to_market) = '6' or length(time_to_market) = '4')
group by article_no,time_to_market) as tab_brand01) as tab_brand02 where rn = 1)
select 
a.article_no, 
b.brand,
c.sex,
d.category,
e.time_to_market,
f.tag_price,
current_timestamp()
from product_article_no a 
left join product_brand b       on a.article_no = b.article_no 
left join product_sex c         on a.article_no = c.article_no 
left join product_category d    on a.article_no = d.article_no 
left join product_season e      on a.article_no = e.article_no 
left join product_tagprice f    on a.article_no = f.article_no 
;
" 2>&1)

if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


 error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D erp_db -N -e "use erp_db;select * from product_trans where brand regexp 'Nike|Jordan';" 2>&1 >/data/exchange/product_trans_nike.txt )
 if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


 error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  product_trans_nike;load data local infile '/data/exchange/product_trans_nike.txt' into table product_trans_nike character set utf8mb4 fields terminated by '\t' lines terminated by '\n';" 2>&1)
 if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi

 error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D erp_db -N -e "use erp_db;select distinct category,sex,fromSize,tosize,CONCAT(fromSize,sex,category) uniq from samp_size_conversion where brand regexp 'Nike|Jordan';" 2>&1 >/data/exchange/samp_size_conversion_nike.txt )
 if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


 error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  samp_size_conversion_nike;load data local infile '/data/exchange/samp_size_conversion_nike.txt' into table samp_size_conversion_nike character set utf8mb4 fields terminated by '\t' lines terminated by '\n';" 2>&1)
 if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "<<<<[product_trans] 处理完成."
fi




##########################################################################################################################
#中文描述： flightclub  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c FlightClub_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/flightclub.csv 2>&1)

sed -i 's/\\\"\"//g' /data/exchange/flightclub.csv
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "mongoDB_flightclub处理完成."
fi

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "drop table if exists flightclub; create table flightclub like flightclub_基础表"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/flightclub.csv 2>&1)

if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "mysql_flightclub处理完成."
fi



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from flightclub group by currency order by count(*) desc;


drop table if exists flightclub_${DATE_STR};create table flightclub_${DATE_STR} as select * from flightclub;

"

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from FlightClub group by currency order by count(*) desc;
-- EUR  欧元         8.1355
-- MXN  墨西哥元     0.39
-- MYR  马来西亚吉特 1.72
-- PHP  菲律宾比索   0.1187
-- SGD  新加坡元     5.42
-- USD  美元         6.97
-- CAD  加元         5.0489
-- TWD  新台币       0.2208



-- truncate table flightclub_基础表 ;
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260330 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260329 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260328 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260327 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260326 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260325 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260324 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260323 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260322 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260321 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260319 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260318 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260317 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260316 where brand regexp 'Nike|Jordan';
-- insert into flightclub_基础表 select * from nike_moss.moss_flightclub_20260315 where brand regexp 'Nike|Jordan';

truncate table flightclub_基础表;
insert into flightclub_基础表 select * from flightclub_${DATE_STR} where brand regexp 'Nike|Jordan';


-- 增加 标准货号转化和 人民币转化

drop table if exists flightclub_清洗01;
create table flightclub_清洗01 as 
select 
*,
LEFT(concat(substring_index(article,' ',1),'-',substring(substring_index(article,' ',-1),1,3)),10) 标准货号,
case 
when currency = 'EUR' then round(price * 8.1355,2) 
when currency = 'MXN' then round(price * 0.39,2) 
when currency = 'MYR' then round(price * 1.72,2) 
when currency = 'PHP' then round(price * 0.1187,2) 
when currency = 'SGD' then round(price * 5.42,2) 
when currency = 'USD' then round(price * 6.97,2) 
when currency = 'CAD' then round(price * 5.0489,2) 
when currency = 'TWD' then round(price * 0.5508,2) 
when currency = 'CNY' then round(price * 1,2) 
when currency = 'JPY' then round(price * 0.0439,2) 
when currency = 'KRW' then round(price * 0.0048,2) 
when currency = 'GBP' then round(price * 9.3391,2) 
when currency = 'AUD' then round(price * 4.8803,2) 
when currency = 'HKD' then round(price * 0.8767,2) 
else 0 
end 金额人民币 
from flightclub_基础表 where article like '%-%';

create index idx_article on flightclub_清洗01(标准货号);



drop table if exists flightclub_清洗02;
CREATE TABLE flightclub_清洗02 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from flightclub_清洗01 a 
left join product_trans_nike  b
on TRIM(a.标准货号) = TRIM(b.article_no);



create index idx_uniq on flightclub_清洗02(uniq);


drop table  if exists  flightclub_清洗03  ;
create table flightclub_清洗03 as 

select a.*,b.tosize 标准尺码 from flightclub_清洗02  a left join samp_size_conversion_nike  b 

on a.uniq =b.uniq;



create index idx_sex on flightclub_清洗03(sex);
create index idx_size on flightclub_清洗03(size);


update flightclub_清洗03 a ,尺码对照表_运营 b 
set a.标准尺码 = b.EUR 
where 
a.sex=b.性别 and a.size = b.us and 
a.标准尺码 is null ;



drop table  if exists  flightclub_结果表  ;
create table flightclub_结果表 as 
select *,substring(insert_time,1,10) 日期,CONCAT(标准货号,'-',标准尺码) sku from flightclub_清洗03 where 标准尺码 is not null and 金额人民币 !=0;

drop table if exists flightclub_结果表sku_${DATE_STR};
CREATE TABLE flightclub_结果表sku_${DATE_STR} AS select * from flightclub_结果表sku;

insert  into flightclub_结果表sku (日期,sku,source,country,inventory_quantity,salescount ,平均销售价)
select 日期,sku,source,country,0 inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from flightclub_结果表 group by 日期,sku,source,country ;

drop table if exists flightclub_结果表article_${DATE_STR};
CREATE TABLE flightclub_结果表article_${DATE_STR} AS select * from flightclub_结果表article;

insert into flightclub_结果表article (日期,标准货号,source,country,inventory_quantity,salescount ,平均销售价)
select 日期,标准货号,source,country,0 inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from flightclub_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  flightclub_运营  ;
create table flightclub_运营 as 
select distinct source,brand,标准货号,size,size_standed,sex,category,标准尺码 from flightclub_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1) 
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else   
  echo "模块 [flightclub] 处理完成，数据已入库 [nike_moss.flightclub]"   
fi
    



##########################################################################################################################
#中文描述： goat  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c goat_${DATE_STR} --fields "brand,货号,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/goat.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else   
sed -i '1s/货号/article/' /data/exchange/goat.csv
sed -i 's/\\\"\"//g' /data/exchange/goat.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "drop table if exists goat;create table goat  like goat_基础表"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/goat.csv 2>&1)
  if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

  fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- select currency,count(*) from goat group by currency order by count(*) desc;
drop table if exists goat_${DATE_STR};create table goat_${DATE_STR} as select * from goat;
"

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table goat_基础表 ;
-- insert into goat_基础表 select * from nike_moss.goat_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into goat_基础表 select * from nike_moss.goat_20260317 where brand regexp 'Nike|Jordan' and price !=0;

truncate table goat_基础表 ;
insert into goat_基础表 select * from nike_moss.goat_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;

drop table if exists goat_清洗01;
create table goat_清洗01 as select *,replace(article,' ','-') 标准货号,round(price * 6.97,2) 金额人民币 from goat_基础表;

create index idx_article on goat_清洗01(标准货号);


drop table if exists goat_清洗02;
CREATE TABLE goat_清洗02 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from goat_清洗01 a 
left join product_trans_nike  b
on a.标准货号 = b.article_no
;


create index idx_uniq on goat_清洗02(uniq);

drop table  if exists  goat_清洗03  ;
create table goat_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.insert_time,a.标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from goat_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

create index idx_sex on goat_清洗03(sex);
create index idx_size on goat_清洗03(size);


update goat_清洗03 a ,尺码对照表_运营 b 
set a.标准尺码 = b.EUR 
where 
a.sex=b.性别 and a.size = b.us and 
a.标准尺码 is null ;

drop table if exists  goat_结果表;
create table goat_结果表 as 
select *,substring(insert_time,1,10) 日期,CONCAT(标准货号,'-',标准尺码) sku from goat_清洗03 where 标准尺码 is not null and 金额人民币 !=0;

drop table if exists goat_结果表sku_${DATE_STR};
create table goat_结果表sku_${DATE_STR} as select * from goat_结果表sku;

insert into goat_结果表sku (日期,sku,source,country,inventory_quantity,salescount ,平均销售价)
select 日期,sku,source,country,0 inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from goat_结果表 group by 日期,sku,source,country ;

drop table if exists goat_结果表article_${DATE_STR};
create table goat_结果表article_${DATE_STR} as select * from goat_结果表article;

insert into goat_结果表article (日期,标准货号,source,country,inventory_quantity,salescount ,平均销售价)
select 日期,标准货号,source,country,0 inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from goat_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  goat_运营;
create table goat_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from goat_清洗03 where 标准尺码 is  null and sex is null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else   
  echo "模块 [goat] 处理完成，数据已入库 [nike_moss.goat]"   
fi



##########################################################################################################################
#中文描述： stock  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c stockx_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/stockx.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi

sed -i 's/\\\"\"//g' /data/exchange/stockx.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  stockx;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/stockx.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from stockx group by currency order by count(*) desc;


drop table if exists stockx_${DATE_STR};create table stockx_${DATE_STR} as select * from stockx;

"

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c stockx_最近购买数据 --fields "购买时间,价格,尺码,货号" --type=csv  --out /data/exchange/stockx最近购买.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/stockx最近购买.csv 

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  stockx最近购买;"


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=购买时间,价格,尺码,货号 \
  nike_moss /data/exchange/stockx最近购买.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table stockx_基础表 ;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260316 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stockx_基础表 select * from nike_moss.stockx_20260315 where brand regexp 'Nike|Jordan' and price !=0;

truncate table stockx_基础表 ;
insert into stockx_基础表 select * from nike_moss.stockx_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;

-- stockx最近购买 统计计算
update stockx最近购买 set 货号= replace(货号,' ','');
update stockx最近购买 set 货号= substring(货号,1,10);
delete from stockx最近购买 where 货号 = '';
delete from stockx最近购买 where length(货号) <10;
delete from stockx最近购买 where substring(货号,7,1) != '-';


drop table if exists stockx最近购买_清洗01;
CREATE TABLE stockx最近购买_清洗01 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.尺码,b.sex,b.category) uniq 
from stockx最近购买 a 
left join product_trans_nike  b
on a.货号 = b.article_no
;

drop table  if exists  stockx最近购买_清洗02  ;
create table stockx最近购买_清洗02 as 
select 
a.*,b.tosize 标准尺码,CONCAT(a.货号,'-',b.tosize) sku 
from stockx最近购买_清洗01  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;


drop table if exists stock_sku销量7天;create table stock_sku销量7天 as  select sku,count(*) stock_sku销量7天  from stockx最近购买_清洗02  WHERE sku is not null and 购买时间 >= NOW() - INTERVAL 7 DAY group by sku;
drop table if exists stock_sku销量30天;create table stock_sku销量30天 as  select sku,count(*) stock_sku销量30天  from stockx最近购买_清洗02  WHERE sku is not null and 购买时间 >= NOW() - INTERVAL 30 DAY group by sku;
drop table if exists stock_货号销量7天;create table stock_货号销量7天 as  select 货号,count(*) stock_货号销量7天  from stockx最近购买_清洗02  WHERE 货号 is not null and 购买时间 >= NOW() - INTERVAL 7 DAY group by 货号;
drop table if exists stock_货号销量30天;create table stock_货号销量30天 as  select 货号,count(*) stock_货号销量30天  from stockx最近购买_清洗02  WHERE 货号 is not null and 购买时间 >= NOW() - INTERVAL 30 DAY group by 货号;

-- stockx最近购买 计算



drop table if exists stockx_清洗01;
create table stockx_清洗01
select *,substring_index(article,'/',1) 货号  from stockx_基础表 where article  like '%/%' union all 
select *,substring_index(article,'/',-1) 货号  from stockx_基础表 where article  like '%/%' union all 
select *,article from stockx_基础表 where article not like '%/%';

update stockx_清洗01 set 货号= substring_index(货号,')',-1) where 货号 like '%)%';
update stockx_清洗01 set 货号= substring(article,1,10) where 货号 = '';
update stockx_清洗01 set  货号 = concat(substring(货号,1,6),'-',substring(货号,7,3))  where 货号 not regexp '-';
update stockx_清洗01 set 货号= replace(货号,' ','');
update stockx_清洗01 set 货号 = replace(substring(article,1,10),' ','-')where LENGTH(货号) !=10 and article like '% %';


delete from stockx_清洗01 where substring(货号,7,1) != '-';
delete from stockx_清洗01 where 货号 = 'TSUT-AF01';
delete from stockx_清洗01 where LENGTH(货号) !=10;


drop table if exists stockx_清洗02;
CREATE TABLE stockx_清洗02 AS 
select a.*,round(a.price * 0.8767,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from stockx_清洗01 a 
left join product_trans_nike  b
on a.货号 = b.article_no
;


drop table  if exists  stockx_清洗03  ;
create table stockx_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.货号 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from stockx_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

create index idx_sex on stockx_清洗03(sex);
create index idx_size on stockx_清洗03(size);

drop table if exists stockx_结果表;
create table stockx_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from stockx_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

Drop table if exists stockx_结果表sku_${DATE_STR};
create table stockx_结果表sku_${DATE_STR} as select * from stockx_结果表sku;


insert into stockx_结果表sku (日期,sku,source,country, inventory_quantity,salescount, 平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from stockx_结果表 group by 日期,sku,source,country ;

Drop table if exists stockx_结果表article_${DATE_STR};
create table stockx_结果表article_${DATE_STR} as select * from stockx_结果表article;

insert into stockx_结果表article (日期,标准货号,source,country,inventory_quantity,salescount ,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from stockx_结果表 group by 日期,标准货号,source,country ;

drop table  if exists  stockx_运营;
create table stockx_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from stockx_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [stockx] 处理完成，数据已入库 [nike_moss.stockx]"
fi


##########################################################################################################################
#中文描述：kream  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c kream_${DATE_STR} --fields "brand,货号,尺码,size_standed,库存,价格,currency,country,source,采集url,pictures_link,insert_time" --type=csv  --out /data/exchange/kream.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi

sed -i '1s/货号/article/' /data/exchange/kream.csv
sed -i '1s/尺码/size/' /data/exchange/kream.csv
sed -i '1s/库存/inventory_quantity/' /data/exchange/kream.csv
sed -i '1s/价格/price/' /data/exchange/kream.csv
sed -i '1s/采集url/acquisition_link/' /data/exchange/kream.csv
sed -i 's/\\\"\"//g' /data/exchange/kream.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  kream;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/kream.csv 2>&1)
  if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
  else
    echo "模块 [kream] 处理完成，数据已入库 [nike_moss.kream]"
  fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from kream group by currency order by count(*) desc;


drop table if exists  kream_${DATE_STR} ;create table kream_${DATE_STR} as select * from kream;

"


error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c kream最近购买 --fields "购买时间,价格,尺码,货号" --type=csv  --out /data/exchange/kream最近购买.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
    echo "模块 [kream最近购买] 处理完成，数据已入库 [nike_moss.kream最近购买]"
fi

sed -i 's/\\\"\"//g' /data/exchange/kream最近购买.csv 

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  kream最近购买;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=购买时间,价格,尺码,货号 \
  nike_moss /data/exchange/kream最近购买.csv 2>&1)
  if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
  else
    echo "模块 [kream最近购买] 处理完成，数据已入库 [nike_moss.kream最近购买]"
  fi

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- kream最近购买 统计计算
update kream最近购买 set 货号= replace(货号,' ','');
update kream最近购买 set 货号= substring(货号,1,10);
delete from kream最近购买 where 货号 = '';
delete from kream最近购买 where length(货号) != 10;
delete from kream最近购买 where substring(货号,7,1) != '-';

update kream最近购买 set 尺码 = substring_index(尺码,'(',1);
update kream最近购买 set 尺码 = substring_index(尺码,'-',1);
update kream最近购买 set 尺码 = 'MISC' where 尺码 like '%ONE%';



drop table if exists kream最近购买_清洗01;
CREATE TABLE kream最近购买_清洗01 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.尺码,b.sex,b.category) uniq 
from kream最近购买 a 
left join product_trans_nike  b
on a.货号 = b.article_no
;

drop table  if exists  kream最近购买_清洗02  ;
create table kream最近购买_清洗02 as 
select 
a.*,b.tosize 标准尺码,CONCAT(a.货号,'-',b.tosize) sku 
from kream最近购买_清洗01  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;


update kream最近购买_清洗02 a ,(select distinct 性别,大类,EUR,KR from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.尺码 =b.KR;
update kream最近购买_清洗02 set 标准尺码= 尺码  where 标准尺码 is null and (尺码 like 'S%' OR 尺码 like 'L%' OR 尺码 like 'M%' OR 尺码 like 'X%' );
update kream最近购买_清洗02 set 标准尺码= substring_index(尺码,'⧸',1)  where 尺码 like '%⧸%';

update kream最近购买_清洗02 set sku=CONCAT(货号,'-',标准尺码) where  sku is null;

delete from kream最近购买_清洗02 where sku is null;


drop table if exists kream_sku销量7天;create table kream_sku销量7天 as  select sku,count(*) kream_sku销量7天  from kream最近购买_清洗02  WHERE sku is not null and 购买时间 >= NOW() - INTERVAL 7 DAY group by sku;
drop table if exists kream_sku销量30天;create table kream_sku销量30天 as  select sku,count(*) kream_sku销量30天  from kream最近购买_清洗02  WHERE sku is not null and 购买时间 >= NOW() - INTERVAL 30 DAY group by sku;
drop table if exists kream_货号销量7天;create table kream_货号销量7天 as  select 货号,count(*) kream_货号销量7天  from kream最近购买_清洗02  WHERE 货号 is not null and 购买时间 >= NOW() - INTERVAL 7 DAY group by 货号;
drop table if exists kream_货号销量30天;create table kream_货号销量30天 as  select 货号,count(*) kream_货号销量30天  from kream最近购买_清洗02  WHERE 货号 is not null and 购买时间 >= NOW() - INTERVAL 30 DAY group by 货号;

-- kream最近购买 计算




-- truncate table kream_基础表 ;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-30' as insert_time from kream_20260330 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-29' as insert_time from kream_20260329 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-28' as insert_time from kream_20260328 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-27' as insert_time from kream_20260327 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-26' as insert_time from kream_20260326 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-25' as insert_time from kream_20260325 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-24' as insert_time from kream_20260324 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-23' as insert_time from kream_20260323 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-20' as insert_time from kream_20260320 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-17' as insert_time from kream_20260317 where price !=0;
-- insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,'2026-03-16' as insert_time from kream_20260316 where price !=0;

truncate table kream_基础表 ;
insert into kream_基础表  select 'nike' as brand, article, size, size_standed, inventory_quantity,price,currency,'韩国' as country, 'kream' as source, acquisition_link, pictures_link,INSERT(INSERT('${DATE_STR}', 5, 0, '-'), 8, 0, '-') as insert_time from kream_${DATE_STR} where price !=0;

drop table if exists kream_清洗01;
create table  kream_清洗01 as 
select *, substring(substring_index(article,'/',1),1,10)  货号 from kream_基础表  where article not like '%/%' union all 
select *, substring(substring_index(article,'/',1),1,10)  货号 from kream_基础表  where article like '%/%' union all 
select *, substring(substring_index(article,'/',-1),1,10) 货号 from kream_基础表  where article like '%/%' ;

update kream_清洗01 set size = substring_index(size,'(',1);
update kream_清洗01 set size = substring_index(size,'-',1);
update kream_清洗01 set size = 'MISC' where size like '%ONE%';




delete  from kream_清洗01 where substring(货号,7,1) != '-';
delete  from kream_清洗01 where length(货号) != 10;





drop table if exists kream_清洗02;
CREATE TABLE kream_清洗02 AS 
select a.*,round(a.price * 0.0047,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from kream_清洗01 a 
left join product_trans_nike  b
on a.货号 = b.article_no;



drop table  if exists  kream_清洗03  ;
create table kream_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.inventory_quantity,a.insert_time,a.货号 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from kream_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;


update kream_清洗03 a ,(select distinct 性别,大类,EUR,KR from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.KR;

update kream_清洗03 set 标准尺码= size  where 标准尺码 is null and (size like 'S%' OR size like 'L%' OR size like 'M%' OR size like 'X%' );
update kream_清洗03 set 标准尺码= substring_index(size,'⧸',1)  where size like '%⧸%';

create index idx_sex on kream_清洗03(sex);
create index idx_size on kream_清洗03(size);

drop table if exists kream_结果表;
create table kream_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from kream_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table if exists kream_结果表sku_${DATE_STR};
create table kream_结果表sku_${DATE_STR} as select * from kream_结果表sku;


insert into kream_结果表sku (日期,sku,source,country,inventory_quantity,salescount,平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from kream_结果表 group by 日期,sku,source,country ;

drop table if exists kream_结果表article_${DATE_STR};
create table kream_结果表article_${DATE_STR} as select * from kream_结果表article;


insert into kream_结果表article (日期,标准货号,source,country,inventory_quantity,salescount,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from kream_结果表 group by 日期,标准货号,source,country ;

drop table  if exists  kream_运营  ;
create table kream_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from kream_清洗03 where 标准尺码 is null ;

 " 2>&1)
 if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
 else
    echo "模块 [kream] 处理完成，数据已入库 [nike_moss.kream]"
 fi
##########################################################################################################################
#中文描述：kickscrew  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c kickscrew_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/kickscrew.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/kickscrew.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  kickscrew;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/kickscrew.csv 2>&1)
  if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

  fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from kickscrew group by currency order by count(*) desc;


drop table if exists kickscrew_${DATE_STR} ;create table kickscrew_${DATE_STR} as select * from kickscrew;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- truncate table kickscrew_基础表 ;
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260330 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260329 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260328 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260327 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260326 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260325 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260324 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260323 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260321 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260320 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260319 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260318 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260317 where brand regexp 'Nike|Jordan';
-- insert into kickscrew_基础表 select * from nike_moss.kickscrew_20260316 where brand regexp 'Nike|Jordan';

truncate table kickscrew_基础表 ;
insert into kickscrew_基础表 select * from nike_moss.kickscrew_${DATE_STR} where brand regexp 'Nike|Jordan';

delete from kickscrew_基础表 where price =0;
delete from kickscrew_基础表 where article ='';
delete from kickscrew_基础表 where length(article) != 10;
delete from kickscrew_基础表 where substring(article,7,1) != '-';

update kickscrew_基础表 set size = substring_index(size,'/',2);
update kickscrew_基础表 set size = replace(size,'MENS/Men\'s','');
update kickscrew_基础表 set size = replace(size,'KIDS/','');
update kickscrew_基础表 set size = replace(size,'WOMENS/Women\'s','');
update kickscrew_基础表 set size = replace(size,'BABY/','');
update kickscrew_基础表 set size = replace(size,'MENS/','');
update kickscrew_基础表 set size = replace(size,'WOWmns','');
update kickscrew_基础表 set size = replace(size,'Mens','');
update kickscrew_基础表 set size = substring_index(size,'UK',1);
delete from kickscrew_基础表 where size not regexp 'US';
update kickscrew_基础表 set size = replace(size,'WOUS','');
update kickscrew_基础表 set size = replace(size,'US','');
delete from kickscrew_基础表 where LENGTH(size) > 6;



-- 港币
drop table if exists kickscrew_清洗02;
CREATE TABLE kickscrew_清洗02 AS 
select a.*,round(a.price * 0.8853,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from kickscrew_基础表 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_article on kickscrew_清洗02(article);

drop table  if exists  kickscrew_清洗03  ;
create table kickscrew_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.inventory_quantity,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from kickscrew_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

update kickscrew_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null;

update kickscrew_清洗03 set 标准尺码= size  where 标准尺码 is null and (size like 'S%' OR size like 'L%' OR size like 'M%' OR size like 'X%' );

create index idx_sex on kickscrew_清洗03(sex);
create index idx_size on kickscrew_清洗03(size);



drop table if exists kickscrew_结果表;
create table kickscrew_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from kickscrew_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  kickscrew_结果表sku_${DATE_STR}  ;
create table kickscrew_结果表sku_${DATE_STR} as select * from kickscrew_结果表sku;

insert into kickscrew_结果表sku (日期,sku,source,country,inventory_quantity,salescount,平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from kickscrew_结果表 group by 日期,sku,source,country ;

drop table  if exists  kickscrew_结果表article_${DATE_STR}  ;
create table kickscrew_结果表article_${DATE_STR} as select * from kickscrew_结果表article;

insert into kickscrew_结果表article (日期,标准货号,source,country,inventory_quantity,salescount,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity,0 salescount ,round(avg(金额人民币),0) 平均销售价 from kickscrew_结果表 group by 日期,标准货号,source,country ;

drop table  if exists  kickscrew_运营  ;
create table kickscrew_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from kickscrew_清洗03 where 标准尺码 is  null ;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [kickscrew] 处理完成，数据已入库 [nike_moss.kickscrew]"   
fi


##########################################################################################################################
#中文描述：musinsa  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c moss_musinsa_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/moss_musinsa.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/moss_musinsa.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  moss_musinsa;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/moss_musinsa.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from moss_musinsa group by currency order by count(*) desc;


drop table if exists moss_musinsa_${DATE_STR};create table moss_musinsa_${DATE_STR} as select * from moss_musinsa;

"

 error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table musinsa_基础表 ;
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260330 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260329 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260328 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260327 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260326 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260325 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260324 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260323 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260322 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260321 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260320 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260319 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260318 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260317 where brand regexp 'Nike|Jordan';
-- insert into musinsa_基础表 select * from nike_moss.moss_musinsa_20260316 where brand regexp 'Nike|Jordan';

truncate table musinsa_基础表;
insert into musinsa_基础表 select * from nike_moss.moss_musinsa_${DATE_STR} where brand regexp 'Nike|Jordan';

delete from musinsa_基础表 where price =0;
delete from musinsa_基础表 where article ='';
delete from musinsa_基础表 where length(article) != 10;
delete from musinsa_基础表 where substring(article,7,1) != '-';

update musinsa_基础表 set size = substring_index(size,'(',1);

delete from musinsa_基础表 where LENGTH(size) > 6;

drop table if exists musinsa_清洗02;
CREATE TABLE musinsa_清洗02 AS 
select a.*,round(a.price * 0.0047,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from musinsa_基础表 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_article on musinsa_清洗02(article);

drop table  if exists  musinsa_清洗03  ;
create table musinsa_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.inventory_quantity,a.salescount,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from musinsa_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

update musinsa_清洗03 a ,(select distinct 性别,大类,EUR,KR from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.KR and 标准尺码  is null;
update musinsa_清洗03 set 标准尺码= 'XXL'  where size = '2XL';

update musinsa_清洗03 set 标准尺码= size  where 标准尺码 is null and (size like 'S%' OR size like 'L%' OR size like 'M%' OR size like 'X%' );

create index idx_sex on musinsa_清洗03(sex);
create index idx_size on musinsa_清洗03(size);



drop table if exists musinsa_结果表;
create table musinsa_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from musinsa_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  musinsa_结果表sku_${DATE_STR};
create table musinsa_结果表sku_${DATE_STR} as select * from musinsa_结果表sku;

insert into musinsa_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from musinsa_结果表 group by 日期,sku,source,country ;

drop table  if exists  musinsa_结果表article_${DATE_STR};
create table musinsa_结果表article_${DATE_STR} as select * from musinsa_结果表article;

insert into musinsa_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from musinsa_结果表 group by 日期,标准货号,source,country ;



drop table  if exists  musinsa_运营  ;
create table musinsa_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from musinsa_清洗03 where 标准尺码 is  null ;
" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [musinsa] 处理完成，数据已入库 [nike_moss.musinsa]"   
fi


##########################################################################################################################
#中文描述：11st  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c 11st_${DATE_STR} --fields "sku,商品ID,salesCount,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/11st.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "MONGODB EXPORT ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/11st.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table 11st;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=sku,商品ID,salesCount,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/11st.csv 2>&1)

if [ $? -ne 0 ]; then
    echo "MYSQL IMPORT ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

drop table if exists 11st_${DATE_STR};create table 11st_${DATE_STR} as select * from 11st;

"

  
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c 11st货号识别 --fields "商品ID,货号,标准尺码,品牌" --type=csv  --out /data/exchange/11st货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "MONGODB EXPORT ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/11st货号识别.csv 



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  11st货号识别;"


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,货号,标准尺码,品牌 \
  nike_moss /data/exchange/11st货号识别.csv 2>&1)

if [ $? -ne 0 ]; then
    echo "MYSQL IMPORT ERROR"
    echo "$mysql_msg"
fi


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table 11st_基础表 ;
-- insert into 11st_基础表 select * from nike_moss.11st_20260330 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260329 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260328 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260327 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260326 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260325 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260324 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260323 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260322 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260321 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260319 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260318 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260317 where brand regexp 'Nike|Jordan';
-- insert into 11st_基础表 select * from nike_moss.11st_20260316 where brand regexp 'Nike|Jordan';

truncate table 11st_基础表 ;
insert into 11st_基础表 select * from nike_moss.11st_${DATE_STR} where brand regexp 'Nike|Jordan';


update 11st_基础表 a,11st货号识别 b set a.article =b.货号 where a.商品ID=b.商品ID;



delete from 11st_基础表 where price =0;
delete from 11st_基础表 where article ='';
delete from 11st_基础表 where size = '';
delete from 11st_基础表 where length(article) != 10;
delete from 11st_基础表 where substring(article,7,1) != '-';
delete from 11st_基础表 where article like '-%';
update 11st_基础表 set inventory_quantity = 1000 where inventory_quantity >9000;


update 11st_基础表 set size = replace(size,' ','');
update 11st_基础表 set size = replace(size,'(','');
update 11st_基础表 set size = replace(size,'FREE/','');
update 11st_基础表 set size = replace(size,'FREE','');

delete from 11st_基础表 where LENGTH(size) > 6;




drop table if exists 11st_清洗02;
CREATE TABLE 11st_清洗02 AS 
select a.*,round(a.price * 0.0047,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from 11st_基础表 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_article on 11st_清洗02(article);

drop table  if exists  11st_清洗03  ;
create table 11st_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.inventory_quantity,a.salescount,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from 11st_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

update 11st_清洗03 a ,(select distinct 性别,大类,EUR,KR from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.KR and 标准尺码  is null;
update 11st_清洗03 set 标准尺码= 'XXL'  where size = '2XL';

update 11st_清洗03 set 标准尺码= size  where 标准尺码 is null and (size like 'S%' OR size like 'L%' OR size like 'M%' OR size like 'X%' );

UPDATE 11st_清洗03 SET 标准尺码 = REGEXP_REPLACE(TRIM(标准尺码), '[0-9]+[)]$', '') WHERE 标准尺码 LIKE '%)' AND 标准尺码 REGEXP '[0-9]';

create index idx_sex on 11st_清洗03(sex);
create index idx_size on 11st_清洗03(size);




drop table if exists 11st_结果表;
create table 11st_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from 11st_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  11st_结果表sku_${DATE_STR};
create table 11st_结果表sku_${DATE_STR} as select * from 11st_结果表sku;

insert into 11st_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from 11st_结果表 group by 日期,sku,source,country ;

drop table  if exists  11st_结果表article_${DATE_STR};
create table 11st_结果表article_${DATE_STR} as select * from 11st_结果表article;

insert into 11st_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from 11st_结果表 group by 日期,标准货号,source,country ;



drop table  if exists  11st_运营  ;
create table 11st_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from 11st_清洗03 where 标准尺码 is  null ;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "MYSQL EXECUTE ERROR"
    echo "$error_msg"
else
  echo "模块 [11st] 处理完成，数据已入库 [nike_moss.11st]"
fi




##########################################################################################################################
#中文描述：coupang  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c coupang_${DATE_STR} --fields "商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/coupang.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/coupang.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  coupang;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/coupang.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from coupang group by currency order by count(*) desc;


drop table if exists coupang_${DATE_STR} ;create table coupang_${DATE_STR} as select * from coupang;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
 SET SESSION sql_mode = '';
-- truncate table coupang_基础表 ;
-- insert into coupang_基础表 select * from nike_moss.coupang_20260330 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260329 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260328 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260327 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260326 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260325 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260324 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260323 where brand regexp 'Nike|Jordan';    
-- insert into coupang_基础表 select * from nike_moss.coupang_20260322 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260321 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260319 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260318 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260317 where brand regexp 'Nike|Jordan';
-- insert into coupang_基础表 select * from nike_moss.coupang_20260316 where brand regexp 'Nike|Jordan';

truncate table coupang_基础表 ;
insert into coupang_基础表 select * from nike_moss.coupang_${DATE_STR} where brand regexp 'Nike|Jordan';

update coupang_基础表 a,coupang货号识别 b set a.article =b.货号 where a.商品ID=b.商品ID;


delete from coupang_基础表 where price =0;
delete from coupang_基础表 where article ='';
delete from coupang_基础表 where size = '';
delete from coupang_基础表 where article like '-%';
delete from coupang_基础表 where article like '0-%';

delete from coupang_基础表 where article like '未识别';
delete from coupang_基础表 where length(article) != 10;
delete from coupang_基础表 where substring(article,7,1) != '-';
delete from coupang_基础表 where article like '-%';
update coupang_基础表 set inventory_quantity = 1000 where inventory_quantity >9000;


update coupang_基础表 set size = replace(size,' ','');
update coupang_基础表 set size = substring_index(size,'(',1);
update coupang_基础表 set size = substring_index(size,'[',1);
update coupang_基础表 set size = substring_index(size,' ×',1);
update coupang_基础表 set size = substring_index(size,'mm',1);
update coupang_基础表 set size = replace(size,'US ',''),size_standed = 'US' WHERE size like 'US %';


drop table if exists coupang_清洗02;
CREATE TABLE coupang_清洗02 AS 
select a.*,round(a.price * 0.0047,2) 金额人民币,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from coupang_基础表 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_article on coupang_清洗02(article);

drop table  if exists  coupang_清洗03  ;
create table coupang_清洗03 as 
select 
a.size,a.size_standed,a.price,a.currency,a.country,a.source,a.inventory_quantity,a.salescount,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from coupang_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq
;

update coupang_清洗03 a ,(select distinct 性别,大类,EUR,KR from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.KR and 标准尺码  is null;

update coupang_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null  AND size_standed = 'US';

update coupang_清洗03 set 标准尺码= 'XXL'  where size = '2XL';

update coupang_清洗03 set 标准尺码= size  where 标准尺码 is null and (size like 'S%' OR size like 'L%' OR size like 'M%' OR size like 'X%' );

create index idx_sex on coupang_清洗03(sex);
create index idx_size on coupang_清洗03(size);




drop table if exists coupang_结果表;
create table coupang_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from coupang_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  coupang_结果表sku_${DATE_STR}  ;
create table coupang_结果表sku_${DATE_STR} as select * from coupang_结果表sku ;


insert into coupang_结果表sku (日期,sku,source,country,inventory_quantity,salescount,平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity,SUM(salescount) salescount ,round(avg(金额人民币),0) 平均销售价 from coupang_结果表 group by 日期,sku,source,country ;

drop table  if exists  coupang_结果表article_${DATE_STR}  ;
create table coupang_结果表article_${DATE_STR} as select * from coupang_结果表article ;


insert into coupang_结果表article (日期,标准货号,source,country,inventory_quantity,salescount,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity,SUM(salescount) salescount ,round(avg(金额人民币),0) 平均销售价 from coupang_结果表 group by 日期,标准货号,source,country ;




drop table  if exists  coupang_运营  ;
create table coupang_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from coupang_清洗03 where 标准尺码 is  null ;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
echo "模块 [coupang] 处理完成，数据已入库 [nike_moss.coupang]"
fi


##########################################################################################################################
#中文描述： mercadolibre  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c mercadolibre_${DATE_STR} --fields "商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/mercadolibre.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/mercadolibre.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  mercadolibre;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/mercadolibre.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from mercadolibre group by currency order by count(*) desc;


drop table if exists mercadolibre_${DATE_STR};create table mercadolibre_${DATE_STR} as select * from mercadolibre;

"


error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c mercadolibre货号识别 --fields "商品ID,货号" --type=csv  --out /data/exchange/mercadolibre货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/mercadolibre货号识别.csv 



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  mercadolibre货号识别;"


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,货号 \
  nike_moss /data/exchange/mercadolibre货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi



error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- truncate table mercadolibre_基础表 ;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into mercadolibre_基础表  select * from mercadolibre_20260322 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260321 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260320 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260319 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260318 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260317 where brand regexp 'Nike|Jordan' and price !=0; 
-- insert into mercadolibre_基础表  select * from mercadolibre_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table mercadolibre_基础表;
insert into mercadolibre_基础表  select * from mercadolibre_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0; 

DROP TABLE IF EXISTS mercadolibre_清洗01;
CREATE TABLE mercadolibre_清洗01 AS 
SELECT 
a.*,
COALESCE(b.货号, a.article) AS real_article,
CASE 
WHEN currency = '阿根廷比索'     THEN price * 0.0050
WHEN currency = '巴西雷亚尔'     THEN price * 1.3190
WHEN currency = '墨西哥比索'     THEN price * 0.3819
WHEN currency = '委内瑞拉玻利瓦尔' THEN price * 0.0148
WHEN currency = '秘鲁新索尔'     THEN price * 1.9845
WHEN currency = '哥伦比亚比索'   THEN price * 0.0018
WHEN currency = '乌拉圭比索'     THEN price * 0.1702
WHEN currency = '智利比索'       THEN price * 0.0075
ELSE 0 
END AS 金额人民币
FROM mercadolibre_基础表 a
LEFT JOIN mercadolibre货号识别 b ON a.商品ID = b.商品ID
WHERE a.price > 0 
AND a.inventory_quantity != 'false'
AND LENGTH(COALESCE(b.货号, a.article)) = 10;

DROP TABLE IF EXISTS mercadolibre_清洗02;
CREATE TABLE mercadolibre_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM mercadolibre_清洗01 a
LEFT JOIN product_trans_nike p ON a.real_article = p.article_no;

create index idx_uniq on mercadolibre_清洗02(uniq);

DROP TABLE IF EXISTS mercadolibre_清洗03;
CREATE TABLE mercadolibre_清洗03 AS 
SELECT 
a.size,
a.size_standed,
a.price,
CASE 
WHEN a.inventory_quantity = 'true' THEN 50 
ELSE CAST(a.inventory_quantity AS DECIMAL) 
END AS inventory_quantity,
a.salesCount AS salescount,
a.currency,
a.country,
a.source,
a.insert_time,
a.article AS 标准货号,
a.金额人民币,
a.sex,
a.category,
a.tag_price,
b.tosize AS 标准尺码 
FROM mercadolibre_清洗02 a 
LEFT JOIN samp_size_conversion_nike b 
ON a.uniq = b.uniq;

create index idx_sex on mercadolibre_清洗03(sex);
create index idx_size on mercadolibre_清洗03(size);

delete from mercadolibre_清洗03 where LENGTH(size) >=8;
update mercadolibre_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;
update mercadolibre_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;

-- select * from mercadolibre_清洗03 where  标准尺码  is null and sex is not null;

update mercadolibre_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;;


update mercadolibre_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;;

drop table if exists mercadolibre_结果表;
create table mercadolibre_结果表 as 
select * ,substring(insert_time,1,10) 日期, TRIM(TRAILING 'EU' FROM CONCAT(标准货号,'-',标准尺码)) sku from mercadolibre_清洗03  where 标准尺码 is not null and 标准尺码 != ''  and 标准货号 != '' and 金额人民币 !=0;

drop table  if exists  mercadolibre_结果表sku_${DATE_STR};
create table mercadolibre_结果表sku_${DATE_STR} as select * from mercadolibre_结果表sku;

insert into mercadolibre_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from mercadolibre_结果表 group by 日期,sku,source,country ;

drop table  if exists  mercadolibre_结果表article_${DATE_STR};
create table mercadolibre_结果表article_${DATE_STR} as select * from mercadolibre_结果表article;

insert into mercadolibre_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from mercadolibre_结果表 group by 日期,标准货号,source,country ;



drop table  if exists  mercadolibre_运营;
create table mercadolibre_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from mercadolibre_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [mercadolibre] 处理完成，数据已入库 [nike_moss.mercadolibre]"
fi


##########################################################################################################################
#中文描述： footlocker  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c moss_footlocker_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/moss_footlocker.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

fi
sed -i 's/\\\"\"//g' /data/exchange/moss_footlocker.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  moss_footlocker;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/moss_footlocker.csv 2>&1)

if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from moss_footlocker group by currency order by count(*) desc;


drop table if exists moss_footlocker_${DATE_STR} ;create table moss_footlocker_${DATE_STR} as select * from moss_footlocker;

"

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- truncate table footlocker_基础表 ;
-- insert into footlocker_基础表  select * from moss_footlocker_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into footlocker_基础表  select * from moss_footlocker_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table footlocker_基础表 ;
insert into footlocker_基础表  select * from moss_footlocker_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;

DROP TABLE IF EXISTS footlocker_清洗01;
CREATE TABLE footlocker_清洗01 AS 
SELECT 
*,
CASE 
WHEN REPLACE(article, ' ', '') NOT REGEXP '-' 
THEN CONCAT(SUBSTRING(REPLACE(article, ' ', ''), 1, 6), '-', SUBSTRING(REPLACE(article, ' ', ''), 7, 3))
ELSE REPLACE(article, ' ', '')
END AS real_article,
CASE 
WHEN currency = '美元' THEN ROUND(price * 6.97, 2)
WHEN currency = '澳元' THEN ROUND(price * 4.7649, 2)
ELSE 0 
END AS 金额人民币,
CASE 
WHEN inventory_quantity = 'false' THEN '0'
WHEN inventory_quantity = 'true' THEN '50'
ELSE 0
END AS 库存 
FROM footlocker_基础表
WHERE price > 0                       
AND inventory_quantity != 'false'   
AND article != ''  ;

DROP TABLE IF EXISTS footlocker_清洗02;
CREATE TABLE footlocker_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM footlocker_清洗01 a
LEFT JOIN product_trans_nike p ON a.real_article = p.article_no;

create index idx_uniq on footlocker_清洗02(uniq);

drop table  if exists  footlocker_清洗03  ;
create table footlocker_清洗03 as 
select 
a.size,a.size_standed,a.price,a.库存 inventory_quantity,a.salescount,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from footlocker_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on footlocker_清洗03(sex);
create index idx_size on footlocker_清洗03(size);

delete from footlocker_清洗03 where LENGTH(size) >=8;
update footlocker_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;
update footlocker_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;

-- select * from footlocker_清洗03 where  标准尺码  is null and sex is not null;

update footlocker_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;;


update footlocker_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;;

drop table if exists footlocker_结果表;
create table footlocker_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from footlocker_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  footlocker_结果表sku_${DATE_STR};
create table footlocker_结果表sku_${DATE_STR} as  select * from footlocker_结果表sku;

insert into footlocker_结果表sku (日期,sku,source,country,inventory_quantity,salescount,平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity,SUM(salescount) salescount ,round(avg(金额人民币),0) 平均销售价 from footlocker_结果表 group by 日期,sku,source,country ;

drop table  if exists  footlocker_结果表article_${DATE_STR};
create table footlocker_结果表article_${DATE_STR} as select * from footlocker_结果表article;


insert into footlocker_结果表article (日期,标准货号,source,country,inventory_quantity,salescount,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity,SUM(salescount) salescount ,round(avg(金额人民币),0) 平均销售价 from footlocker_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  footlocker_运营;
create table footlocker_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from footlocker_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [footlocker] 处理完成，数据已入库 [nike_moss.footlocker]"
fi


##########################################################################################################################
#中文描述： zalando  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c moss_zalando_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/moss_zalando.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/moss_zalando.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  moss_zalando;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/moss_zalando.csv 2>&1)

if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from moss_zalando group by currency order by count(*) desc;


drop table if exists moss_zalando_${DATE_STR};create table moss_zalando_${DATE_STR} as select * from moss_zalando;

"

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bs <<EOF >> /opt/scripts/test/nike_moss_run.log 2>&1
-- truncate table zalando_基础表 ;
-- insert into zalando_基础表  select * from moss_zalando_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into zalando_基础表  select * from moss_zalando_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table zalando_基础表 ;
insert into zalando_基础表  select * from moss_zalando_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;


DROP TABLE IF EXISTS zalando_清洗01;

CREATE TABLE zalando_清洗01 AS 
SELECT *,case 
when currency = '欧元' then round(price * 8.1355,2)
else 0 
end 金额人民币 
FROM zalando_基础表 
WHERE (brand REGEXP 'nike|Jordan')
AND price > 0 
AND inventory_quantity > 0 
AND article != '' 
AND article NOT LIKE '%/%';


UPDATE zalando_清洗01 
SET article = REPLACE(REPLACE(article, '"', ''), ' ', '');

UPDATE zalando_清洗01 
SET article = CONCAT(SUBSTRING(article, 1, 6), '-', SUBSTRING(article, 7, 3))
WHERE article NOT REGEXP '-';

DROP TABLE IF EXISTS zalando_清洗02;
CREATE TABLE zalando_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM zalando_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;

create index idx_uniq on zalando_清洗02(uniq);

drop table  if exists  zalando_清洗03  ;
create table zalando_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from zalando_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on zalando_清洗03(sex);
create index idx_size on zalando_清洗03(size);

delete from zalando_清洗03 where LENGTH(size) >=8;
update zalando_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;
update zalando_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;

select * from zalando_清洗03 where  标准尺码  is null and sex is not null;

update zalando_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;;


update zalando_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;;

drop table if exists zalando_结果表;
create table zalando_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from zalando_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  zalando_结果表sku_${DATE_STR};
create table zalando_结果表sku_${DATE_STR} as select * from zalando_结果表sku;

insert into zalando_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from zalando_结果表 group by 日期,sku,source,country ;

drop table  if exists  zalando_结果表article_${DATE_STR};
create table zalando_结果表article_${DATE_STR} as select * from zalando_结果表article;

insert into zalando_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from zalando_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  zalando_运营;
create table zalando_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from zalando_清洗03 where 标准尺码 is  null and sex is not null;
EOF

if [ $? -ne 0 ]; then
    echo "zalando ERROR"
else
  echo "模块 [zalando] 处理完成，数据已入库 [nike_moss.zalando]"
fi


##########################################################################################################################
#中文描述： gmarket  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c moss_gmarket_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/moss_gmarket.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/moss_gmarket.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  moss_gmarket;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/moss_gmarket.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from moss_gmarket group by currency order by count(*) desc;


drop table if exists moss_gmarket_${DATE_STR} ;create table moss_gmarket_${DATE_STR} as select * from moss_gmarket;

"

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- truncate table gmarket_基础表 ;
-- insert into gmarket_基础表  select * from moss_gmarket_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into gmarket_基础表  select * from moss_gmarket_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table gmarket_基础表 ;
insert into gmarket_基础表  select * from moss_gmarket_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;


DROP TABLE IF EXISTS gmarket_清洗01;

CREATE TABLE gmarket_清洗01 AS 
SELECT 
brand,
article,
size,
size_standed,
price,
round(price * 0.004752,2) 金额人民币,
CASE 
WHEN CAST(inventory_quantity AS SIGNED) > 1000 THEN 1000 
ELSE CAST(inventory_quantity AS SIGNED) 
END AS inventory_quantity,
currency ,
country ,
source ,
insert_time
FROM gmarket_基础表 
WHERE
price > 0                
AND inventory_quantity >= 0     
AND article != '';


DROP TABLE IF EXISTS gmarket_清洗02;
CREATE TABLE gmarket_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM gmarket_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;

create index idx_uniq on gmarket_清洗02(uniq);

drop table  if exists  gmarket_清洗03  ;
create table gmarket_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from gmarket_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on gmarket_清洗03(sex);
create index idx_size on gmarket_清洗03(size);

delete from gmarket_清洗03 where LENGTH(size) >=8;
update gmarket_清洗03 set 标准尺码 = size where size_standed ='JP' and 标准尺码  is null and sex is not null;
update gmarket_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;

update gmarket_清洗03 a ,(select distinct 性别,大类,EUR,JP from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.JP and 标准尺码  is null and sex is not null;

drop table if exists gmarket_结果表;
create table gmarket_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from gmarket_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  gmarket_结果表sku_${DATE_STR};
create table gmarket_结果表sku_${DATE_STR} as select * from gmarket_结果表sku;

insert into gmarket_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from gmarket_结果表 group by 日期,sku,source,country ;

drop table  if exists  gmarket_结果表article_${DATE_STR};
create table gmarket_结果表article_${DATE_STR} as select * from gmarket_结果表article;

insert into gmarket_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from gmarket_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  gmarket_运营;
create table gmarket_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from gmarket_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [gmarket] 处理完成，数据已入库 [nike_moss.gmarket]"
fi



##########################################################################################################################
#中文描述： stadiumgoods  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c StadiumGoods_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/stadiumgoods.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi

sed -i 's/\\\"\"//g' /data/exchange/stadiumgoods.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  stadiumgoods;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/stadiumgoods.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from stadiumgoods group by currency order by count(*) desc;


drop table if exists  stadiumgoods_${DATE_STR} ;create table stadiumgoods_${DATE_STR} as select * from stadiumgoods;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- truncate table stadiumgoods_基础表 ;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into stadiumgoods_基础表  select * from stadiumgoods_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table stadiumgoods_基础表 ;
insert into stadiumgoods_基础表  select * from stadiumgoods_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;


DROP TABLE IF EXISTS stadiumgoods_清洗01;

CREATE TABLE stadiumgoods_清洗01 AS 
SELECT 
brand,
article,
size,
size_standed,
price,
round(price * 6.97,2) 金额人民币,
inventory_quantity,
currency ,
country ,
source ,
insert_time
FROM stadiumgoods_基础表 
WHERE 
price > 0                
AND inventory_quantity >= 0     
AND article != '';

DROP TABLE IF EXISTS stadiumgoods_清洗02;
CREATE TABLE stadiumgoods_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM stadiumgoods_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;


create index idx_uniq on stadiumgoods_清洗02(uniq);

drop table  if exists  stadiumgoods_清洗03  ;
create table stadiumgoods_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from stadiumgoods_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on stadiumgoods_清洗03(sex);
create index idx_size on stadiumgoods_清洗03(size);

delete from stadiumgoods_清洗03 where LENGTH(size) >=8;
update stadiumgoods_清洗03 set 标准尺码 = size where size_standed ='US' and 标准尺码  is null and sex is not null;
update stadiumgoods_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;


update stadiumgoods_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;

drop table if exists stadiumgoods_结果表;    
create table stadiumgoods_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from stadiumgoods_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  stadiumgoods_结果表sku_${DATE_STR};
create table stadiumgoods_结果表sku_${DATE_STR} as select * from stadiumgoods_结果表sku;

insert into stadiumgoods_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from stadiumgoods_结果表 group by 日期,sku,source,country ;

drop table  if exists  stadiumgoods_结果表article_${DATE_STR};
create table stadiumgoods_结果表article_${DATE_STR} as select * from stadiumgoods_结果表article;

insert into stadiumgoods_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from stadiumgoods_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  stadiumgoods_运营;
create table stadiumgoods_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from stadiumgoods_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
  echo "模块 [stadiumgoods] 处理完成，数据已入库 [nike_moss.stadiumgoods]"
fi
 

##########################################################################################################################
#中文描述： lazada  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c lazada_${DATE_STR} --fields "商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/lazada.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/lazada.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  lazada;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/lazada.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c lazada货号识别 --fields "商品ID,货号" --type=csv  --out /data/exchange/lazada货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/lazada货号识别.csv 

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  lazada货号识别;"


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,货号 \
  nike_moss /data/exchange/lazada货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from lazada group by currency order by count(*) desc;


drop table if exists lazada_${DATE_STR};create table lazada_${DATE_STR} as select * from lazada;

"



error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table lazada_基础表 ;
-- insert into lazada_基础表  select * from lazada_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into lazada_基础表  select * from lazada_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table lazada_基础表;
insert into lazada_基础表  select * from lazada_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;



DROP TABLE IF EXISTS lazada_清洗01;
CREATE TABLE lazada_清洗01 AS 
SELECT 
商品ID,
brand,
article,
size,
size_standed,
price,
case 
when currency = 'PHP' then round(price * 0.118,2)
when currency = 'VND' then round(price * 0.00026,2) 
when currency = 'THB' then round(price * 0.2242,2) 
when currency = 'IDR' then round(price * 0.0004,2) 
when currency = 'SGD' then round(price * 5.4745,2)  
when currency = 'MYR' then round(price * 1.7384,2) 
else 0 
end 金额人民币 ,
inventory_quantity,
currency ,
country ,
source ,
insert_time
FROM lazada_基础表 
WHERE
price > 0                
AND inventory_quantity >= 0     
AND article != '';

update  lazada_清洗01 a,lazada货号识别 b
set a.article = b.货号 
where a.商品ID=b.商品ID;

DROP TABLE IF EXISTS lazada_清洗02;
CREATE TABLE lazada_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM lazada_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;

UPDATE lazada_清洗02 SET size = TRIM(SUBSTRING_INDEX(size, ':', -1)) WHERE size LIKE '%:%';

create index idx_uniq on lazada_清洗02(uniq);

drop table  if exists  lazada_清洗03  ;
create table lazada_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from lazada_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;


create index idx_sex on lazada_清洗03(sex);
create index idx_size on lazada_清洗03(size);

delete from lazada_清洗03 where LENGTH(size) >=8;
update lazada_清洗03 set 标准尺码 = size where size_standed ='US' and 标准尺码  is null and sex is not null;
update lazada_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;


update lazada_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;

drop table if exists lazada_结果表;    
create table lazada_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from lazada_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  lazada_结果表sku_${DATE_STR};
create table lazada_结果表sku_${DATE_STR} as  SELECT * FROM lazada_结果表sku;

INSERT INTO lazada_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from lazada_结果表 group by 日期,sku,source,country ;

drop table  if exists  lazada_结果表article_${DATE_STR};
create table lazada_结果表article_${DATE_STR} as SELECT * FROM lazada_结果表article;


INSERT INTO lazada_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from lazada_结果表 group by 日期,标准货号,source,country;


drop table  if exists  lazada_运营;
create table lazada_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from lazada_清洗03 where 标准尺码 is  null and sex is not null;
" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [lazada] 处理完成，数据已入库 [nike_moss.lazada]"
fi


##########################################################################################################################
#中文描述： cdiscount  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c cdiscount_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/cdiscount.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

fi
sed -i 's/\\\"\"//g' /data/exchange/cdiscount.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  cdiscount;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/cdiscount.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from cdiscount group by currency order by count(*) desc;


drop table if exists cdiscount_${DATE_STR};create table cdiscount_${DATE_STR} as select * from cdiscount;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
-- truncate table cdiscount_基础表 ;
-- insert into cdiscount_基础表  select * from cdiscount_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into cdiscount_基础表  select * from cdiscount_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table cdiscount_基础表 ;
insert into cdiscount_基础表  select * from cdiscount_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;


DROP TABLE IF EXISTS cdiscount_清洗01;

CREATE TABLE cdiscount_清洗01 AS 
SELECT 
brand,
article,
size,
size_standed,
price,
round(price * 7.9182,2) 金额人民币,
inventory_quantity,
currency ,
country ,
source ,
insert_time
FROM cdiscount_基础表 
WHERE 
price > 0                
AND inventory_quantity >= 0     
AND article != '';

DROP TABLE IF EXISTS cdiscount_清洗02;
CREATE TABLE cdiscount_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM cdiscount_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;

create index idx_uniq on cdiscount_清洗02(uniq);

drop table  if exists  cdiscount_清洗03  ;
create table cdiscount_清洗03 as  
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from cdiscount_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;


create index idx_sex on cdiscount_清洗03(sex);
create index idx_size on cdiscount_清洗03(size);

delete from cdiscount_清洗03 where LENGTH(size) >=8;
update cdiscount_清洗03 set 标准尺码 = size where size_standed ='US' and 标准尺码  is null and sex is not null;
update cdiscount_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;


update cdiscount_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;

drop table if exists cdiscount_结果表;    
create table cdiscount_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from cdiscount_清洗03  where 标准尺码 is not null and 金额人民币 !=0;


drop table  if exists  cdiscount_结果表sku_${DATE_STR};
create table cdiscount_结果表sku_${DATE_STR} as select * from cdiscount_结果表sku;

insert into cdiscount_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from cdiscount_结果表 group by 日期,sku,source,country ; 

drop table  if exists  cdiscount_结果表article_${DATE_STR};
create table cdiscount_结果表article_${DATE_STR} as select * from cdiscount_结果表article;

insert into cdiscount_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from cdiscount_结果表 group by 日期,标准货号,source,country ;



drop table  if exists  cdiscount_运营;
create table cdiscount_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from cdiscount_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [cdiscount] 处理完成，数据已入库 [nike_moss.cdiscount]"
fi
##########################################################################################################################
#中文描述： trendyol  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c trendyol_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/trendyol.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

fi

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  trendyol;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/trendyol.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"

fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from trendyol group by currency order by count(*) desc;


drop table if exists trendyol_${DATE_STR};create table trendyol_${DATE_STR} as select * from trendyol;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table trendyol_基础表 ;
-- insert into trendyol_基础表  select * from trendyol_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into trendyol_基础表  select * from trendyol_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table trendyol_基础表 ;
insert into trendyol_基础表  select * from trendyol_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;


DROP TABLE IF EXISTS trendyol_清洗01;

CREATE TABLE trendyol_清洗01 AS 
SELECT 
brand,
REGEXP_REPLACE(article, 'Ya.', '') as article,
size,
size_standed,
price,
round(price * 0.1553,2) 金额人民币,
inventory_quantity,
currency ,
country ,
source ,
insert_time
FROM trendyol_基础表 
WHERE 
price > 0                
AND inventory_quantity >= 0     
AND article != '';

DROP TABLE IF EXISTS trendyol_清洗02;
CREATE TABLE trendyol_清洗02 AS 
SELECT 
a.*, 
p.sex, p.category, p.tag_price,
CONCAT(a.size, p.sex, p.category) AS uniq
FROM trendyol_清洗01 a
LEFT JOIN product_trans_nike p ON a.article = p.article_no;

create index idx_uniq on trendyol_清洗02(uniq);

drop table  if exists  trendyol_清洗03  ;
create table trendyol_清洗03 as  
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from trendyol_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on trendyol_清洗03(sex);
create index idx_size on trendyol_清洗03(size);

update trendyol_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;

drop table if exists trendyol_结果表;    
create table trendyol_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from trendyol_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  trendyol_结果表sku_${DATE_STR};
create table trendyol_结果表sku_${DATE_STR} as select * from trendyol_结果表sku;

insert into trendyol_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from trendyol_结果表 group by 日期,sku,source,country ;

drop table  if exists  trendyol_结果表article_${DATE_STR};
create table trendyol_结果表article_${DATE_STR} as select * from trendyol_结果表article;

insert into trendyol_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from trendyol_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  trendyol_运营;
create table trendyol_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from trendyol_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [trendyol] 处理完成，数据已入库 [nike_moss.trendyol]"
fi

##########################################################################################################################
#中文描述： 亚马逊  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c 亚马逊_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time,ASIN" --type=csv  --out /data/exchange/亚马逊.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [亚马逊] 处理完成，数据已入库 [nike_moss.亚马逊]"
fi

sed -i 's/\\\"\"//g' /data/exchange/亚马逊.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  亚马逊;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time,ASIN \
  nike_moss /data/exchange/亚马逊.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from 亚马逊 group by currency order by count(*) desc;


drop table if exists  亚马逊_${DATE_STR};create table 亚马逊_${DATE_STR} as select * from 亚马逊;

"



error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c 亚马逊货号识别 --fields "ASIN,货号" --type=csv  --out /data/exchange/亚马逊货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [亚马逊货号识别] 处理完成，数据已入库 [nike_moss.亚马逊货号识别]"
fi

sed -i 's/\\\"\"//g' /data/exchange/亚马逊货号识别.csv 

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  亚马逊货号识别;"


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=ASIN,货号 \
  nike_moss /data/exchange/亚马逊货号识别.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi



error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "
truncate table  亚马逊_基础表;
insert into 亚马逊_基础表  select * from 亚马逊_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;
update 亚马逊_基础表 set insert_time =  INSERT(INSERT('${DATE_STR}', 5, 0, '-'), 8, 0, '-') ;

update  亚马逊_基础表 a,亚马逊货号识别 b
set a.article = b.货号 
where a.ASIN=b.ASIN;




delete from 亚马逊_基础表 where price like '0%';
delete from 亚马逊_基础表 where article = '未识别'; -- 5316
delete  from 亚马逊_基础表 where substring(article,7,1) != '-';
delete  from 亚马逊_基础表 where length(article) != 10;

--  非数字销量 0

update 亚马逊_基础表 set salescount = 0 where salescount = '';
update 亚马逊_基础表 set salescount = replace(salescount,',','') ;
update 亚马逊_基础表 set salescount = replace(salescount,'.','') ;
update 亚马逊_基础表 set salescount = 0 where salesCount like '% %';
update 亚马逊_基础表 set salescount = replace(salescount,'K','00');
update 亚马逊_基础表 set salescount = 0 WHERE salescount NOT REGEXP '^[0-9]+$';


update 亚马逊_基础表 set inventory_quantity = 1000 where inventory_quantity='9999';
update 亚马逊_基础表 set inventory_quantity = 100  where inventory_quantity='999';

-- select salescount,count(*) from 亚马逊_基础表 group by salescount order by salescount desc;

-- with test as (select concat(country,currency) cc,count(*) from 亚马逊_基础表 group by concat(country,currency)  having count(*) <200)
-- delete from 亚马逊_基础表 where concat(country,currency) in (select cc from test);


update 亚马逊_基础表 set size = 'MISC' WHERE  size = '均码';
update 亚马逊_基础表 set size_standed = 'UK' WHERE  size like '%UK%';
update 亚马逊_基础表 set size_standed = 'US' WHERE  size like '%US%'; 
update 亚马逊_基础表 set size_standed = 'EU' WHERE  size like '%EU%';     
update 亚马逊_基础表 set size = substring_index(size,' ',1);

-- select country,count(*) from 亚马逊_基础表 group by country order by count(*) desc;


DROP TABLE IF EXISTS 亚马逊_清洗01;
CREATE TABLE 亚马逊_清洗01 AS
SELECT
    brand,
    price,
    article,
    inventory_quantity, 
    size, 
    size_standed,
    insert_time,
    asin,
    country,
    currency,
    salescount,
    article 标准货号,
    CASE
        WHEN country='西班牙' THEN round(price * 7.80,2)
        WHEN country='巴西' THEN round(price * 1.3155,2)
        WHEN country='意大利' THEN round(price * 7.80,2)
        WHEN country='法国' THEN round(price * 7.80,2)
        WHEN country='波兰' THEN round(price * 1.8438 ,2)
        WHEN country='德国' THEN round(price * 7.80,2)
        WHEN country='加拿大' THEN round(price * 4.9542,2)
        WHEN country='墨西哥' THEN round(price * 0.3817,2)
        WHEN country='荷兰' THEN round(price * 7.80,2)
        WHEN country='比利时' THEN round(price * 7.80,2)
        WHEN country='瑞典' THEN round(price * 0.7213,2)
        WHEN country='澳大利亚' THEN round(price * 4.7336,2)
        WHEN country='爱尔兰' THEN round(price * 7.80,2)
        WHEN country='美国' THEN round(price * 6.9083,2)
        WHEN country='阿拉伯联合酋长国' THEN round(price * 1.8811,2)
        WHEN country='新加坡' THEN round(price * 5.3505,2)
        WHEN country='英国' THEN round(price * 9.1203,2)
        WHEN country='沙特阿拉伯' THEN round(price * 1.8409,2)
        WHEN country='日本' THEN round(price * 0.0433,2)
        WHEN country='土耳其' THEN round(price * 0.1553,2)
        WHEN country='印度' THEN round(price * 0.0734,2)
        WHEN country='南非' THEN round(price * 0.403,2)
        ELSE 0
    END AS 金额人民币
FROM 亚马逊_基础表
WHERE salescount IS NOT NULL 
  AND salescount != '' 
  AND TRIM(salescount) != ''
  AND salescount REGEXP '[0-9]';
    
    
drop table if exists 亚马逊_清洗02;
CREATE TABLE 亚马逊_清洗02 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from 亚马逊_清洗01 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_uniq on 亚马逊_清洗02(uniq);

drop table  if exists  亚马逊_清洗03  ;
create table 亚马逊_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.salesCount salescount,a.currency,a.country,'亚马逊' source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from 亚马逊_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on 亚马逊_清洗03(sex);
create index idx_size on 亚马逊_清洗03(size);

delete from 亚马逊_清洗03 where LENGTH(size) >=8;


update 亚马逊_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;
update 亚马逊_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;


update 亚马逊_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null and a.size_standed='US';


update 亚马逊_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null and a.size_standed='UK';



drop table if exists 亚马逊_结果表;
create table 亚马逊_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from 亚马逊_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  亚马逊_结果表sku_${DATE_STR};
create table 亚马逊_结果表sku_${DATE_STR} as select * from 亚马逊_结果表sku;

insert into 亚马逊_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from 亚马逊_结果表 group by 日期,sku,source,country ;

drop table  if exists  亚马逊_结果表article_${DATE_STR};
create table 亚马逊_结果表article_${DATE_STR} as select * from 亚马逊_结果表article;

insert into 亚马逊_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from 亚马逊_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  亚马逊_运营;
create table 亚马逊_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from 亚马逊_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [亚马逊] 处理完成，数据已入库 [nike_moss.亚马逊]"
fi


##########################################################################################################################
#中文描述： letian  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}
DATE_STR=$(date -d "1 days ago" "+%Y%m%d")

error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c letian_${DATE_STR} --fields "商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/letian.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [letian] 处理完成，数据已入库 [nike_moss.letian]"
fi

sed -i 's/\\\"\"//g' /data/exchange/letian.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  letian;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=商品ID,brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/letian.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from letian group by currency order by count(*) desc;

drop table if exists letian_${DATE_STR};create table letian_${DATE_STR} as select * from letian;

"


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


update  letian_${DATE_STR} a,乐天货号识别 b
set a.article = b.货号 
where a.商品ID=b.商品ID;

delete from letian_${DATE_STR} where article = '';

truncate table  letian_基础表;
insert into  letian_基础表 select * from letian_${DATE_STR} where brand regexp 'nike|Jordan' ;
update letian_基础表 set insert_time = '${BEGIN_DATE}' ;



delete from letian_基础表 where price =0;
delete from letian_基础表 where article = '未识别';
delete from letian_基础表 where article = '';
delete  from letian_基础表 where substring(article,7,1) != '-';
delete  from letian_基础表 where length(article) != 10;

--  非数字销量 0

update letian_基础表 set salescount = 0 where salescount = '';
update letian_基础表 set salescount = 0 WHERE salescount NOT REGEXP '^[0-9]+$';
update letian_基础表 set inventory_quantity = 1000 where inventory_quantity='9999';
update letian_基础表 set inventory_quantity = 100  where inventory_quantity='999';

-- select size,count(*) from letian_基础表 group by size order by count(*) desc;


update letian_基础表 set size = substring_index(size,'(',1);
update letian_基础表 set size_standed = 'UK' WHERE  size like '%UK%';
update letian_基础表 set size_standed = 'US' WHERE  size like '%US%'; 
update letian_基础表 set size_standed = 'EU' WHERE  size like '%EU%';   
update letian_基础表 set size = substring_index(size,'-',1);       
update letian_基础表 set size = substring_index(size,'cm',1);  
update letian_基础表 set size = replace(size,'US M ','');  
update letian_基础表 set size = replace(size,'US ','');    
update letian_基础表 set size = replace(size,'US_','');    
update letian_基础表 set size = replace(size,'W ',''); 
update letian_基础表 set size = replace(size,'W','');  

update letian_基础表 set size = replace(size,'M_',''); 
update letian_基础表 set size = replace(size,'_','');  
update letian_基础表 set size = replace(size,'US',''); 

update letian_基础表 set size = substring_index(size,'/',1);

-- select country,count(*) from letian_基础表 group by country order by count(*) desc;


DROP TABLE IF EXISTS letian_清洗01;
CREATE TABLE letian_清洗01 AS
SELECT
brand,
price,
article,
inventory_quantity, 
size, 
size_standed,
insert_time,
country,
currency,
salescount,
article 标准货号,
CASE WHEN country='日本' THEN round(price * 0.0433,2) ELSE 0 END AS 金额人民币
FROM letian_基础表;
    
    
drop table if exists letian_清洗02;
CREATE TABLE letian_清洗02 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from letian_清洗01 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_uniq on letian_清洗02(uniq);

drop table  if exists  letian_清洗03  ;
create table letian_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.salesCount salescount,a.currency,a.country,'letian' source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from letian_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on letian_清洗03(sex);
create index idx_size on letian_清洗03(size);

delete from letian_清洗03 where LENGTH(size) >=8;


update letian_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;


update letian_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null and a.size_standed='US';


update letian_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null and a.size_standed='UK';

update letian_清洗03 a ,(select distinct 性别,大类,EUR,JP from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.JP and 标准尺码  is null and sex is not null and a.size_standed='';


drop table if exists letian_结果表;
create table letian_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from letian_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  letian_结果表sku_${DATE_STR};
create table letian_结果表sku_${DATE_STR} as select * from letian_结果表sku;

insert into letian_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from letian_结果表 group by 日期,sku,source,country ;

drop table  if exists  letian_结果表article_${DATE_STR};
create table letian_结果表article_${DATE_STR} as select * from letian_结果表article;

insert into letian_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from letian_结果表 group by 日期,标准货号,source,country ;


drop table  if exists  letian_运营;
create table letian_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from letian_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
    echo "模块 [letian] 处理完成，数据已入库 [nike_moss.letian]"
fi
##########################################################################################################################
#中文描述：ebay  快速开发
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据抽取
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：${day_zs02}

DATE_STR=$(date -d "1 days ago" "+%Y%m%d")
error_msg=$(mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d moss -c moss_ebay_${DATE_STR} --fields "brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time" --type=csv  --out /data/exchange/moss_ebay.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi
sed -i 's/\\\"\"//g' /data/exchange/moss_ebay.csv

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "truncate table  moss_ebay;"

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
error_msg=$(mysqlimport \
  --host=DB_HOST \
  --user=root \
  --pPASS \
  --local \
  --fields-terminated-by=',' \
  --fields-enclosed-by='"' \
  --lines-terminated-by='\n' \
  --ignore-lines=1 \
  --columns=brand,article,size,size_standed,inventory_quantity,price,salesCount,currency,country,source,acquisition_link,pictures_link,insert_time \
  nike_moss /data/exchange/moss_ebay.csv 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "


-- select currency,count(*) from moss_ebay group by currency order by count(*) desc;


drop table if exists moss_ebay_${DATE_STR};create table moss_ebay_${DATE_STR} as select * from moss_ebay;

"

error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss --local-infile -Bse "

-- truncate table moss_ebay_基础表 ;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260330 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260329 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260328 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260327 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260326 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260325 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260324 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260323 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260322 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260321 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260320 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260319 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260318 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260317 where brand regexp 'Nike|Jordan' and price !=0;
-- insert into moss_ebay_基础表  select * from moss_ebay_20260316 where brand regexp 'Nike|Jordan' and price !=0;

truncate table moss_ebay_基础表 ;
insert into moss_ebay_基础表  select * from moss_ebay_${DATE_STR} where brand regexp 'Nike|Jordan' and price !=0;

delete  from moss_ebay_基础表 where substring(article,7,1) != '-';
delete  from moss_ebay_基础表 where length(article) != 10;
delete  from moss_ebay_基础表 where article = '';
delete  from moss_ebay_基础表 where article LIKE '-%';
delete  from moss_ebay_基础表 where article LIKE '0-%';
delete  from moss_ebay_基础表 where article LIKE '1-%';

DROP TABLE IF EXISTS ebay_清洗01;
CREATE TABLE ebay_清洗01 AS 
SELECT brand,article,
size, size_standed, inventory_quantity, price, salesCount, currency, country, source, acquisition_link, insert_time,
CASE currency 
WHEN '美元' THEN ROUND(price * 6.97, 2)
WHEN '欧元' THEN ROUND(price * 8.09, 2) 
WHEN '澳元' THEN ROUND(price * 4.66, 2) 
WHEN '英镑' THEN ROUND(price * 9.34, 2) 
WHEN '加元' THEN ROUND(price * 5.01, 2)  
WHEN '瑞士法郎' THEN ROUND(price * 8.8112, 2) 
WHEN '港币' THEN ROUND(price * 0.8946, 2) 
ELSE 0 
END AS 金额人民币 
FROM moss_ebay_基础表 ;

CREATE INDEX idx_article_price ON ebay_清洗01 (article);

UPDATE ebay_清洗01 set size = replace(size,'US ','');
UPDATE ebay_清洗01 set size = replace(size,'UK ','');
UPDATE ebay_清洗01 set size = substring_index(size,' ',1);
UPDATE ebay_清洗01 set size = substring_index(size,'/',1);
UPDATE ebay_清洗01 set size = substring_index(size,'=',1);
UPDATE ebay_清洗01 set size = substring_index(size,'(',1);
UPDATE ebay_清洗01 set size = substring_index(size,'.0',1);
UPDATE ebay_清洗01 set size = replace(size,',','');
update ebay_清洗01 set size = REPLACE(size,'US',''),size_standed = 'US' where size like '%US%';
update ebay_清洗01 set size = REPLACE(size,'UK',''),size_standed = 'UK' where size like '%UK%';


drop table if exists ebay_清洗02;
CREATE TABLE ebay_清洗02 AS 
select a.*,b.sex,b.category,b.tag_price,CONCAT(a.size,b.sex,b.category) uniq 
from ebay_清洗01 a 
left join product_trans_nike  b
on a.article = b.article_no;

create index idx_uniq on ebay_清洗02(uniq);

drop table  if exists  ebay_清洗03  ;
create table ebay_清洗03 as 
select 
a.size,a.size_standed,a.price,a.inventory_quantity,a.salesCount salescount,a.currency,a.country,a.source,a.insert_time,a.article 标准货号,a.金额人民币,a.sex,a.category,a.tag_price,b.tosize 标准尺码 
from ebay_清洗02  a 
left join samp_size_conversion_nike  b 
on a.uniq = b.uniq;

create index idx_sex on ebay_清洗03(sex);
create index idx_size on ebay_清洗03(size);

delete from ebay_清洗03 where LENGTH(size) >=8;
update ebay_清洗03 set 标准尺码 = size where size_standed ='EU' and 标准尺码  is null and sex is not null;
update ebay_清洗03 set 标准尺码 = size where size_standed ='' and 标准尺码  is null and sex is not null;

-- select * from ebay_清洗03 where  标准尺码  is null and sex is not null;

update ebay_清洗03 a ,(select distinct 性别,大类,EUR,US from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.US and 标准尺码  is null and sex is not null;;


update ebay_清洗03 a ,(select distinct 性别,大类,EUR,UK from 尺码对照表_运营) b 
set a.标准尺码 = b.EUR 
where a.category='SHOE'  and a.sex= b.性别 and a.size =b.UK and 标准尺码  is null and sex is not null;;



drop table if exists ebay_结果表;
create table ebay_结果表 as 
select * ,substring(insert_time,1,10) 日期, CONCAT(标准货号,'-',标准尺码) sku from ebay_清洗03  where 标准尺码 is not null and 金额人民币 !=0;

drop table  if exists  ebay_结果表sku_${DATE_STR};
create table ebay_结果表sku_${DATE_STR} as select * from ebay_结果表sku;

insert into ebay_结果表sku (日期,sku,source,country,inventory_quantity,平均销售价)  
select 日期,sku,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from ebay_结果表 group by 日期,sku,source,country ;

drop table  if exists  ebay_结果表article_${DATE_STR};
create table ebay_结果表article_${DATE_STR} as select * from ebay_结果表article;

insert into ebay_结果表article (日期,标准货号,source,country,inventory_quantity,平均销售价)  
select 日期,标准货号,source,country,sum(inventory_quantity) inventory_quantity ,round(avg(金额人民币),0) 平均销售价 from ebay_结果表 group by 日期,标准货号,source,country ;

update  ebay_结果表sku a,ebay国家中文对应表 b
set a.country = b.国家 
where a.country=b.country;

update  ebay_结果表article a,ebay国家中文对应表 b
set a.country = b.国家 
where a.country=b.country;

drop table  if exists  ebay_运营;
create table ebay_运营 as 
select distinct source,'NIKE' brand,标准货号,size,size_standed,sex,category,标准尺码 from ebay_清洗03 where 标准尺码 is  null and sex is not null;

" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
echo "模块 [ebay] 处理完成，数据已入库 [nike_moss.ebay]"
fi


error_msg=$(mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D nike_moss_new --local-infile -Bse "


DROP TABLE IF EXISTS flightclub_结果表sku;
Create table if not exists flightclub_结果表sku as
    SELECT sku, 平均销售价, inventory_quantity,salescount,source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount,  source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.flightclub_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS stockx_结果表sku;
create table if not exists stockx_结果表sku as
    SELECT sku, 平均销售价, inventory_quantity,salescount,source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.stockx_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS kream_结果表sku;
create table if not exists kream_结果表sku as
    SELECT sku, 平均销售价, inventory_quantity,salescount,source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.kream_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS goat_结果表sku;
create table if not exists goat_结果表sku as
    SELECT sku, 平均销售价, inventory_quantity,salescount,source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.goat_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS ebay_结果表sku;
create table if not exists ebay_结果表sku as
    SELECT sku, 平均销售价, inventory_quantity,salescount,source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.ebay_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS gmarket_结果表sku;
create table if not exists gmarket_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.gmarket_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS stadiumgoods_结果表sku;
create table if not exists stadiumgoods_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.stadiumgoods_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS 亚马逊_结果表sku;
create table if not exists 亚马逊_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价, inventory_quantity, salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.亚马逊_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS lazada_结果表sku;
create table if not exists lazada_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.lazada_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS letian_结果表sku;
create table if not exists letian_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.letian_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS mercadolibre_结果表sku;
create table if not exists mercadolibre_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.mercadolibre_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS footlocker_结果表sku;
create table if not exists footlocker_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.footlocker_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS zalando_结果表sku;
create table if not exists zalando_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.zalando_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS coupang_结果表sku;
create table if not exists coupang_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM ( 
        SELECT sku, 平均销售价,inventory_quantity,salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.coupang_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS 11st_结果表sku;
create table if not exists 11st_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.11st_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS musinsa_结果表sku;
create table if not exists musinsa_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.musinsa_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS kickscrew_结果表sku;
create table if not exists kickscrew_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价,inventory_quantity,salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.kickscrew_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS cdiscount_结果表sku;
create table if not exists cdiscount_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价, inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.cdiscount_结果表sku) t WHERE sn = 1;
DROP TABLE IF EXISTS trendyol_结果表sku;
create table if not exists trendyol_结果表sku as
    SELECT sku, 平均销售价,inventory_quantity,salescount, source FROM (
        SELECT sku, 平均销售价, inventory_quantity,null as salescount, source, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY 日期 DESC) as sn FROM nike_moss.trendyol_结果表sku) t WHERE sn = 1;



drop table if exists nike_new结果表sku;
create table  nike_new结果表sku as 
select 
a.*,
b.平均销售价  flightclub_价格,
b.inventory_quantity  flightclub_库存,
b.salescount  flightclub_销量,
c.平均销售价  stockx_价格,
c.inventory_quantity  stockx_库存,
c.salescount  stockx_销量,
c1.stock_sku销量7天  stockx_销量7天,
c2.stock_sku销量30天  stockx_销量30天,
d.平均销售价  kream_价格,
d.inventory_quantity  kream_库存,
d.salescount  kream_销量,
d1.kream_sku销量7天  kream_销量7天,
d2.kream_sku销量30天  kream_销量30天,
e.平均销售价  goat_价格,
e.inventory_quantity  goat_库存,
e.salescount  goat_销量,
f.平均销售价  ebay_价格,
f.inventory_quantity  ebay_库存,
f.salescount  ebay_销量,
g.平均销售价  gmarket_价格,
g.inventory_quantity  gmarket_库存,
h.平均销售价  stadiumgoods_价格,
h.inventory_quantity  stadiumgoods_库存,
i.平均销售价  亚马逊_价格,
i.inventory_quantity  亚马逊_库存,
i.salescount  亚马逊_销量,
j.平均销售价  lazada_价格,
j.inventory_quantity  lazada_库存,
k.平均销售价  letian_价格,
k.inventory_quantity  letian_库存,
k.salescount  letian_销量,
l.平均销售价  mercadolibre_价格,
l.inventory_quantity  mercadolibre_库存,
m.平均销售价  footlocker_价格,
m.inventory_quantity  footlocker_库存,
m.salescount  footlocker_销量,
n.平均销售价  zalando_价格,
n.inventory_quantity  zalando_库存,
o.平均销售价  coupang_价格,
o.inventory_quantity  coupang_库存,
o.salescount  coupang_销量,
p.平均销售价  11st_价格,
p.inventory_quantity  11st_库存,
q.平均销售价  musinsa_价格,
q.inventory_quantity  musinsa_库存,
q.salescount  musinsa_销量,
r.平均销售价  kickscrew_价格,
r.inventory_quantity  kickscrew_库存,
r.salescount  kickscrew_销量,
s.平均销售价  cdiscount_价格,
s.inventory_quantity  cdiscount_库存,
t.平均销售价  trendyol_价格,
t.inventory_quantity  trendyol_库存



from 原始表 a 


left join flightclub_结果表sku b on a.标准SKU=b.sku
left join stockx_结果表sku c  on a.标准SKU=c.sku
left join nike_moss.stock_sku销量7天 c1 on a.标准SKU=c1.sku
left join nike_moss.stock_sku销量30天 c2 on a.标准SKU=c2.sku
left join kream_结果表sku d  on a.标准SKU=d.sku 
left join nike_moss.kream_sku销量7天 d1 on a.标准SKU=d1.sku
left join nike_moss.kream_sku销量30天 d2 on a.标准SKU=d2.sku
left join goat_结果表sku e  on a.标准SKU=e.sku 
left join ebay_结果表sku f  on a.标准SKU=f.sku 
left join gmarket_结果表sku g on a.标准SKU=g.sku 
left join stadiumgoods_结果表sku h on a.标准SKU=h.sku 
left join 亚马逊_结果表sku i on a.标准SKU=i.sku 
left join lazada_结果表sku j on a.标准SKU=j.sku 
left join letian_结果表sku k on a.标准SKU=k.sku 
left join mercadolibre_结果表sku l on a.标准SKU=l.sku 
left join footlocker_结果表sku m on a.标准SKU=m.sku 
left join zalando_结果表sku n on a.标准SKU=n.sku 
left join coupang_结果表sku o on a.标准SKU=o.sku 
left join 11st_结果表sku p on a.标准SKU=p.sku 
left join musinsa_结果表sku q on a.标准SKU=q.sku
left join kickscrew_结果表sku r on a.标准SKU=r.sku 
left join cdiscount_结果表sku s on a.标准SKU=s.sku 
left join trendyol_结果表sku t on a.标准SKU=t.sku;



" 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR"
    echo "$error_msg"
else
echo "模块 [nike_moss结果表] 处理完成，数据已入库 [nike_moss_new.nike_moss结果表]"
fi
echo "-----------------"

