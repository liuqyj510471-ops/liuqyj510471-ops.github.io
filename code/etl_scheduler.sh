#! /bin/sh

date 
export ACCUMULO_HOME=/var/lib/accumulo
dbname="device-c2cf9e590beaca2d-data"
username="USER"
password='PASS'
ip="INFLUXDB_HOST"
port="8086"
day_id=`date -d yesterday +%Y-%m-%d`
day_zs=`date '+%Y-%m-%d'`
day_zs02=`date '+%Y%m%d'`
deldate=`date -d"7 day ago" +%Y%m%d`
day_starttime=${day_id:0:4}-01-01
time1=`date +%s`
time2=`date -d"14 day ago" +%s`
time3=`date -d"1 day ago" +%s`

d3=$(((${time3}+3600*8)/86400*86400-3600*8))'000'
d1=$(((${time1}+3600*8)/86400*86400-3600*8))'000'
d2=$(((${time2}+3600*8)/86400*86400-3600*8))'000'
startTime=`date '+%Y-%m-%d %H:%M:%S'`
startTime_s=`date +%s`

logs="/var/log/etl/rugen_${day_zs02}.log"
errologs="/var/log/etl/erro_rugen_${day_zs02}.log"



##备注：hudong_批量退订单明细_财务财务更新_不删           互动售后退款表  统计后财务确认 批量打款 这个表需要定时更新

##########################################################################################################################
#中文描述：自动化ETL调度 结果数据备份 按照 年月日时分秒  建立备份表
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：20240306

current_date=$(date +"%Y-%m-%d %H:%M:%S")
year=${current_date:0:4} 
month=${current_date:5:2} 
day=${current_date:8:2} 
hour=${current_date:11:2}
minute=${current_date:14:2}
second=${current_date:17:2}

echo $year$month$day$hour$minute$second

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
use big_data;
create table bak_rugen_result_sale_order_$year$month$day$hour$minute$second as 
select * from rugen_result_sale_order;
"

if [ $? -eq 0 ]
then

    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "1   数据热备份成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}

else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 1  数据热备份失败失败   ">> ${errologs}   

fi






##########################################################################################################################
#中文描述：ERP销售单 基础表 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#
#开发时间 ：202401
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;

-- 上面已经计算完成一遍了 不需要每天重复计算  现在逻辑是每天全脸计算 逻辑优化  将2025年01月到202508月数据放到  result_sale_order01_202501_202508 


-- result_sale_order01  这个表只需要每天计算最近整月月的数据   再将result_sale_order01_202501_202508 数据插入就可以

truncate table result_sale_order01;
insert into result_sale_order01 
SELECT
    FROM_UNIXTIME(floor(a.order_date_time/1000)) date_time,   -- 下单时间
    a.name order_no,                                           -- so_order_id
    a.sale_channel_id,                                       -- 店铺id
    c.channel_name AS sales_channels,                         -- 店铺 
    a.state AS state,                                         -- 订单状态
    case when a.origin_state='TRADE_FINISHED' then '交易成功' else a.origin_state end origin_state,--  销售渠道状态
    a.ps_gx_order_state,                                     -- 渠道状态
    a.origin AS source,                                       -- 基础订单号
    case when a.origin_order_multiple=0 then '单' 
         when a.origin_order_multiple=1 then '多' 
         end  platform_system_number,                                                            -- 订单数量
    if(a.ps_gx_order_no is null,y.oneProductPickingMyjoCode,a.ps_gx_order_no)  ps_gx_order_no,  -- 平台单号

    a.picking_source AS pickingSource,                       -- 平台
    e.pw_name AS psGxWarehouseChannelName,                   -- 配货仓库
    b.default_code AS oneProductCode,                        -- 商家编码
    a.pay_amount AS payAmount,                               -- 销售价
    a.one_product_cost_price AS costprice,                   -- 成本价
    a.express_number,                                        -- 发货快递
    a.express_company_code,                                  -- 发货快递公司名称
    e.is_refund AS is_refund,                                 -- 是否支持退换货
    a.contact_phone,                                         -- 收件人手机号
    a.buyer_open_uid,                                        -- 买家id
    a.oaid,                                                  -- 收件人id
    a.customer_name                                          -- 收件人名称

FROM
    erp_db.sale_order_new a
    LEFT JOIN erp_db.product  b ON a.one_product_id = b.id
    LEFT JOIN 

    (SELECT mj_barcode oneProductPickingMyjoCode,origin_id FROM erp_db.stock_pickup  WHERE type = 'SALE_ORDER' GROUP BY origin_id )  y ON a.id = y.origin_id 

    LEFT JOIN erp_db.sale_channel c ON a.sale_channel_id = c.id
    LEFT JOIN erp_db.stock_warehouse d ON a.picking_warehouse_id = d.id
    LEFT JOIN erp_db.samp_warehouse e ON a.ps_gx_warehouse_channel = e.id 

-- WHERE FROM_UNIXTIME(floor(a.order_date_time/1000)) >= '2026-03-01 00:00:00' 

where a.order_date_time >=1772294400000 

order by a.order_date_time desc;

insert into result_sale_order01 select * from result_sale_order01_202602ETL调度;



update result_sale_order01 set origin_state = '交易成功' where ps_gx_order_no = '3-已结算' and origin_state='DOU_DIAN_FINISH';
update result_sale_order01 set ps_gx_order_no = '' where ps_gx_order_no = 'NULL';
update result_sale_order01 set pickingsource = '' where pickingsource = 'NULL';
update result_sale_order01 set psGxWarehouseChannelName = '' where psGxWarehouseChannelName = 'NULL';



"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 2   从聚石塔获取ERP销售单数据   并更新订单装状态 成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 2   从聚石塔获取ERP销售单数据 失败   ">> ${errologs}


fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from result_sale_order01;" >/data/exchange/result_sale_order01.txt

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 3  销售单数据导出本地 成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "  3  销售单数据导出本地   失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  result_sale_order01;load data local infile '/data/exchange/result_sale_order01.txt' into table result_sale_order01 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 4  ERP销售单基础数据获取  etl 到 本机服务器  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 4   ERP销售单基础数据获取  etl 到本机服务器  失败   ">> ${errologs}   
    
fi


##  BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  mysql -hRDS_HOST_2 -uroot -pPASS -D dugdb --local-infile -Bse "truncate table  result_sale_order01;load data local infile '/data/exchange/result_sale_order01.txt' into table result_sale_order01 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"







##########################################################################################################################
#中文描述：同业数据实时更新分析
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401



##  同业支付宝账单  etl  IP_ADDR  rugen 库
##
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -N -e "
select 
distinct 
tong_ye_ali_pay_info_id,
occurrence_timestr date_time,
income_amount,
expenditure_amount,
business_serial_number,
merchant_order_number,
goods_name,
type_of_service account_type,
remark 
from erp_db.tong_ye_ali_pay_statement_of_account_info 
where occurrence_timestr >='2026-01-01 00:00:00' ;" >/data/exchange/ty01.txt


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "6   聚石塔同业  机器人 账单数据拉取 成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "6  聚石塔同业  机器人 账单数据拉取 失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  ty01;load data local infile '/data/exchange/ty01.txt' into table ty01 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "7  聚石塔 同业机器人 账单数据  同步到 本机 服务器 成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 7  聚石塔 同业机器人 账单数据  同步到 本机 服务器  失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "


truncate table 同业支付宝2号和4号_rd汇款单号人工匹配;


insert into 同业支付宝2号和4号_rd汇款单号人工匹配 
select distinct x.merchant_order_number_update,x.merchant_order_number from 
(

select 
a.business_serial_number,a.merchant_order_number,
b.merchant_order_number merchant_order_number_update
 from 

(select business_serial_number,merchant_order_number,income_amount,expenditure_amount from ty01) a 
inner join 
(select business_serial_number,merchant_order_number,income_amount,expenditure_amount from ty01) b 

on a.business_serial_number=b.business_serial_number and a.merchant_order_number !=b.merchant_order_number 

where a.merchant_order_number like 'RD%'

order by a.business_serial_number
) x ;


insert into 同业支付宝2号和4号_rd汇款单号人工匹配 
select distinct x.merchant_order_number_update,x.merchant_order_number from 
(

select 
a.business_serial_number,a.merchant_order_number,
b.merchant_order_number merchant_order_number_update
 from 

(select business_serial_number,merchant_order_number,income_amount,expenditure_amount from ty01) a 
inner join 
(select business_serial_number,merchant_order_number,income_amount,expenditure_amount from ty01) b 

on a.business_serial_number=b.business_serial_number and a.merchant_order_number !=b.merchant_order_number 

where a.merchant_order_number like 'RM%'

order by a.business_serial_number
) x ;




update ty01 a,同业支付宝2号和4号_rd汇款单号人工匹配 b 
set a.merchant_order_number = b.merchant_order_number 
where a.merchant_order_number=b.merchant_order_number_rd;

update  ty01 set merchant_order_number = substring_index(remark,'T200P',-1) where merchant_order_number not like '%T200P%' and remark like '%T200P%';


-- 这一步是解决 相同 buiness_serial_number 下 有多个 merchant_order_number  一个带T200P 一个不带   将不带T200P 的 merchant_order_number  替换成带的
drop table if exists 同业按照交易订单号匹配_商户订单号 ;
create table 同业按照交易订单号匹配_商户订单号 as 
with test01 as (
with test as (
select a.business_serial_number,a.merchant_order_number,ROW_NUMBER() over (partition by a.business_serial_number order by a.merchant_order_number desc) sn 
from (select distinct business_serial_number,merchant_order_number from ty01) a  )
select business_serial_number from test where sn=2)

select * from ty01 
where business_serial_number in (select business_serial_number from test01) order by business_serial_number desc ;


update ty01 a,
(select distinct business_serial_number,merchant_order_number FROM ty01 where business_serial_number in (select distinct business_serial_number from 同业按照交易订单号匹配_商户订单号) and merchant_order_number like 'T200P%') b 

set a.merchant_order_number=b.merchant_order_number 

where a.business_serial_number = b.business_serial_number;


update ty01 
set merchant_order_number = substring_index(merchant_order_number,'_',1) 
where merchant_order_number like '%\_%'  and (tong_ye_ali_pay_info_id = 2 or tong_ye_ali_pay_info_id = 4) 
and (merchant_order_number like '39%' or merchant_order_number like '38%' or merchant_order_number like '40%');

update ty01 
set merchant_order_number = replace(merchant_order_number,'T200P','') where merchant_order_number like 'T200P%';
update ty01 set merchant_order_number = substring(replace(remark,'保险理赔-卖家版运费险理赔 订单号:',''),1,19) where remark like '保险理赔-卖家版运费险理赔%';
update ty01 set merchant_order_number = substring(replace(remark,'保险理赔-聚划算运费险理赔 订单号:',''),1,19) where remark like '保险理赔-聚划算运费险理赔%';

 
update ty01 set merchant_order_number = substring_index(remark,'T200P',-1) where 
(merchant_order_number like 'I-capital%' or 
merchant_order_number like 'F-refundplatform3%' or 
merchant_order_number like 'F-capital-pPASS or 
merchant_order_number = 'CPPAYMENTCONFIRM20024042800568366' or 
merchant_order_number like 'F-capital-general%' or 
merchant_order_number like 'assign_account%'
) 
and remark like '%T200P%';

update ty01 set merchant_order_number = replace(remark,'支付宝转账小额打款-关联订单号：','') where remark like '支付宝转账小额打款-关联订单号：%';
 
update ty01 set merchant_order_number = substring(goods_name,1,18)  where goods_name like 'OTP%';


 update ty01 set merchant_order_number = replace(goods_name,'唯品会订单-订单编号','') 
  where (tong_ye_ali_pay_info_id=63 or tong_ye_ali_pay_info_id = 64) and goods_name like '唯品会订单-订单编号%';

"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "8   同业机器人数据  清洗 成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 8   同业机器人数据  清洗 失败   ">> ${errologs}   
    
fi



## 从mongo 中获取 下单日志 和订单日志信息

##   备注： 这一段代码 因为下午运行 数据mongo 数据库资源冲突  所以上午运行
##   后期如果mongo 资源好了  再恢复

mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 手机群控 -c 下单日志 --fields "ERP订单号,下单状态,平台订单号,交易编号,原平台订单号,原交易编号,付款时间/秒,支付金额,手机名称" --type=csv  --out /data/exchange/下单日志.csv
sed -i 's/\\\"\"//g' /data/exchange/下单日志.csv



## 将csv 数据加载到 数据库中
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

truncate table  下单日志;load data local infile '/data/exchange/下单日志.csv' into table 下单日志  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " mongo 下单日志数据 加载到数据库成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "mongo 下单日志数据 加载到数据库失败   ">> ${errologs}   
    
fi





mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 手机群控 -c 订单日志 --fields "订单编号,交易编号,订单创建时间,订单状态,尺码,货号,金额" --type=csv  --out /data/exchange/订单日志.csv
sed -i 's/\\\"\"//g' /data/exchange/订单日志.csv


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

truncate table  订单日志;load data local infile '/data/exchange/订单日志.csv' into table 订单日志  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " mongo 订单日志数据 加载到数据库成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "mongo 订单日志数据 加载到数据库失败   ">> ${errologs}   
    
fi














BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen  -e "

truncate table  交易编号_订单编号;

insert into 交易编号_订单编号 
select distinct x.交易编号,x.订单编号 from 
(
select distinct 交易编号,订单编号 from 订单日志 union all 
select distinct 交易编号,平台订单号 订单编号 from 下单日志) x 

where x.交易编号 is not null and x.订单编号 is not null;


delete from 交易编号_订单编号 where 交易编号 = '';
delete from 交易编号_订单编号 where 订单编号 = '';


truncate table  原交易编号_原订单编号;
insert into 原交易编号_原订单编号 
select distinct 原交易编号,原平台订单号 from 下单日志 where 原交易编号 is not null;


-- delete from 交易编号_订单编号 where 交易编号 in (select 交易编号 from 原交易编号_原订单编号) and 订单编号 in (select 订单编号 from 原交易编号_原订单编号);
insert into 交易编号_订单编号  select 交易编号,订单编号 from 原交易编号_原订单编号;

drop table if exists 交易编号_订单编号_bak;
create table 交易编号_订单编号_bak as 
select distinct * from 交易编号_订单编号;

truncate table 交易编号_订单编号;
insert into 交易编号_订单编号  select * from 交易编号_订单编号_bak;



SET GLOBAL group_concat_max_len=102400;
SET group_concat_max_len=102400;

truncate table  下单编号_so;
insert into 下单编号_so select 平台订单号,group_concat(distinct substring(ERP订单号,1,11)) so订单号 from 下单日志 where 平台订单号 is not null and 平台订单号 != '' and ERP订单号 like 'SO%' group by 平台订单号;
insert into 下单编号_so select 原平台订单号,group_concat(distinct substring(ERP订单号,1,11)) so订单号 from 下单日志 where 原平台订单号 is not null and 原平台订单号 != '' and ERP订单号 like 'SO%' group by 原平台订单号;
insert into 下单编号_so select '110148486480474540','SO005987691';
insert into 下单编号_so select '110149620138479414','SO006040584';

drop table if exists 下单编号_so_bak;
create table 下单编号_so_bak as select distinct * from 下单编号_so;

truncate table 下单编号_so;
insert into 下单编号_so select * from 下单编号_so_bak;

"




if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "12  mongo 下单数据  加载到本机服务器 成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "12  mongo 下单数据  加载到本机服务器 失败   ">> ${errologs}   
    
fi









## 将研发正式库luim 中 同业识货拼多多 支付宝数据 迁移到  数据数据库
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -N -e "use luim;select merchant_order_number,occurrence_timestr,income_amount,expenditure_amount,type_of_service,'111111111111111111111' order_no,now() from tong_ye_ali_pay_statement_of_account_info where tong_ye_ali_pay_info_id=67 and merchant_order_number like 'XP%' ;" > /data/exchange/麦腾_同业识货拼多多_支付宝账单明细表.txt


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "13   将研发正式库luim 中 同业识货拼多多 支付宝数据 迁移到  数据数据库   成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 13  将研发正式库luim 中 同业识货拼多多 支付宝数据 迁移到  数据数据库 失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  麦腾_同业识货拼多多_支付宝账单明细表;load data local infile '/data/exchange/麦腾_同业识货拼多多_支付宝账单明细表.txt' into table 麦腾_同业识货拼多多_支付宝账单明细表 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "14   将研发正式库luim 中 同业识货拼多多 支付宝数据加载到  数据数据库  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 14  将研发正式库luim 中 同业识货拼多多 支付宝数据 加载到  数据数据库  失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "

drop table if exists 麦腾_同业识货拼多多_支付宝账单明细表_middle;
CREATE TABLE 麦腾_同业识货拼多多_支付宝账单明细表_middle (
  merchant_order_number varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '商户订单号',
  occurrence_timestr varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '发生时间',
  income_amount varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '收入金额',
  expenditure_amount varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '支出金额',
  type_of_service varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '业务类型',
  order_no varchar(21) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'so单号',
  create_time varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

insert into 麦腾_同业识货拼多多_支付宝账单明细表_middle 
select * from  麦腾_同业识货拼多多_支付宝账单明细表 where type_of_service = '在线支付';

drop table if exists 麦腾_同业识货拼多多_下单日志;
CREATE TABLE 麦腾_同业识货拼多多_下单日志 (
  so_order_id varchar(32),
  pay_time varchar(255) DEFAULT NULL,
  pay decimal(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
insert into 麦腾_同业识货拼多多_下单日志 
select ERP订单号 so_order_id,付款时间_秒 pay_time,支付金额 pay from 下单日志 where 付款时间_秒 is not null and 手机名称 = '拼多多下单机器人1';


delete from 麦腾_同业识货拼多多_下单日志 where pay_time = 'NaT';

update 麦腾_同业识货拼多多_支付宝账单明细表_middle x,
(SELECT a.*,b.so_order_id 
from       麦腾_同业识货拼多多_支付宝账单明细表_middle a 
left join  麦腾_同业识货拼多多_下单日志 b 
on 
abs(a.expenditure_amount)=b.pay and 
TIMESTAMPDIFF(SECOND,pay_time,occurrence_timestr)>0 and TIMESTAMPDIFF(SECOND,pay_time,occurrence_timestr)<60) y 
set x.order_no = y.so_order_id 
where x.merchant_order_number=y.merchant_order_number
;


/*
update 麦腾_同业识货拼多多_支付宝账单明细表_middle x,
(SELECT a.*,b.so_order_id 
from       麦腾_同业识货拼多多_支付宝账单明细表_middle a 
left join  麦腾_同业识货拼多多_下单日志 b 
on 
TIMESTAMPDIFF(SECOND,pay_time,occurrence_timestr)>0 and TIMESTAMPDIFF(SECOND,pay_time,occurrence_timestr)<60) y 

set x.order_no = y.so_order_id 
where x.merchant_order_number=y.merchant_order_number and x.order_no is null;
*/

update 麦腾_同业识货拼多多_支付宝账单明细表 a,
麦腾_同业识货拼多多_支付宝账单明细表_middle b 
set a.order_no = b.order_no 
where a.merchant_order_number=b.merchant_order_number;

-- 将 没有匹配上so单号   但是支付宝交易中   收支 抵消的订单删除
delete from 麦腾_同业识货拼多多_支付宝账单明细表 where merchant_order_number in 
(select p.merchant_order_number from (
select merchant_order_number,count(*) from 麦腾_同业识货拼多多_支付宝账单明细表 where order_no is null group by merchant_order_number having count(*) >1) p);

update 麦腾_同业识货拼多多_支付宝账单明细表 a,
       麦腾_同业识货拼多多_人工补充表 b 
set a.order_no = b.so_order_id 
where a.merchant_order_number = b.merchant_order_number;


drop table if exists 麦腾_同业识货拼多多_成本结果表;
create table 麦腾_同业识货拼多多_成本结果表 as 
select substring(order_no,1,11) order_no,sum(income_amount+expenditure_amount) pay,
group_concat(distinct merchant_order_number separator'~') orderid from 麦腾_同业识货拼多多_支付宝账单明细表 
where order_no like 'so%' 
group by substring(order_no,1,11);
"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "15   同业识货拼多多  支付宝数据计算 成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 15   同业识货拼多多  支付宝数据计算  失败   ">> ${errologs}   
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "

delete from ty01  
where 
account_type = '提现' or  
account_type like '充值%' or 
account_type like '收费%' or 
account_type like '账户开户' or 
(account_type = '其它' and remark = '商户资金管家-资金管家' )
;

update ty01 
set merchant_order_number=substring_index(remark,'T200P',-1) 
where remark like '%T200P%';




-- 这里 交易编号_订单编号 会有一个 交易编号 对应多个订单编号问题   例如：安心购  
-- 需要人工处理   一个交易编码 任意取一个 订单编号对应


drop table if exists ty_bak;
create table ty_bak as 
select a.*,b.订单编号 
from ty01 a 
left join 
(select m.交易编号,m.订单编号 from (
select 交易编号,订单编号,ROW_NUMBER() over (partition by 交易编号 order by 订单编号 desc) sn  from 交易编号_订单编号) m where m.sn=1) b
on a.merchant_order_number=b.交易编号 ;




update ty_bak 
set merchant_order_number = 订单编号 
where 订单编号 is not null;

delete from ty_bak 
where account_type='其它' and merchant_order_number ='';




drop table if exists ty_01;
create table ty_01 as 
select 
a.*,
b.so订单号  
from ty_bak a 
left join 
(select 平台订单号,so订单号 from 下单编号_so) b 
on a.merchant_order_number=b.平台订单号 
;


-- 删除没有实际支出的订单
delete from ty_01 where 
merchant_order_number in 
(
select a.merchant_order_number from 
(select merchant_order_number , sum(income_amount + -abs(expenditure_amount)) 实际支出 from ty_01 where so订单号 is null group by merchant_order_number) a 
where a.实际支出 = 0)
;




-- 这一块代码是补充  20241119--20241120 同业支付宝因为处理售后问题被得物封号  人工下单 关联出来的so订单号


update ty_01 a,1119_1120同业人工下单订单号核对 b 
set a.so订单号=b.so订单号 
where a.merchant_order_number= b.商户订单号;



update ty_01 set 订单编号 = substring(replace(remark,'保险理赔-退货宝补偿-订单号:',''),1,19) where remark like '保险理赔-退货宝补偿-订单号:%';

update ty_01 set merchant_order_number = substring(replace(remark,'保险理赔-退货宝补偿-订单号:',''),1,19) where remark like '保险理赔-退货宝补偿-订单号:%';
update ty_01 set merchant_order_number = substring(replace(remark,'保险理赔-退换货运费险理赔-订单号:',''),1,19) where remark like '保险理赔-退换货运费险理赔-订单号:%';
delete from ty_01 where so订单号 is null and remark like '%代发';
delete from ty_01 where so订单号 is null and remark like 'SO%';
delete from ty_01 where so订单号 is null and account_type ='转账';
delete from ty_01 where so订单号 is null and tong_ye_ali_pay_info_id in (62,67,88,89);
delete from ty_01 where so订单号 is null and account_type = '其它' and remark = '淘宝消费者保证金-支付-交易赔付-违背承诺-违背发货承诺-延迟发货';
delete from ty_01 where business_serial_number like '202407%' and so订单号 is null;

SET GLOBAL group_concat_max_len=102400;
SET group_concat_max_len=102400;
update ty_01 a,
(select  ps_gx_order_no,substring(group_concat(distinct order_no),1,11) so订单号 from result_sale_order01 where ps_gx_order_no is not null and ps_gx_order_no !='' and pickingSource like '%TONGYE%' group by ps_gx_order_no) b 

set a.so订单号 = b.so订单号 
where a.merchant_order_number=b.ps_gx_order_no and a.so订单号 is null;


update ty_01 a,rugen.同业人工对应表 b 
set a.so订单号 = b.so单号 
where 
a.merchant_order_number =b.下单号 and 
a.so订单号 is null;


-- 查出对应不上so单号的 同业扣款 交给人工审核 并收取反馈结果  更新到   同业人工对应表上

drop table if exists 同业对应不上so单号_人工核对;
create table 同业对应不上so单号_人工核对  as 
with test as (
select a.*,b.merchant_order_number yuan 
from (select *  from ty_01 where so订单号 is null and remark != '企业红包-淘宝现金红包提现' and remark != '商户资金管家-资金管家' ) a 
left join ty01 b 
on 
a.tong_ye_ali_pay_info_id = b.tong_ye_ali_pay_info_id and 
a.date_time = b.date_time and 
a.business_serial_number = b.business_serial_number) 

select * from test 
where yuan not in (select distinct 交易编号 from 交易编号_订单编号) and 
      yuan not in (select distinct 交易编号 from 原交易编号_原订单编号) and 
      yuan not in (select distinct distinct 下单号 from 同业人工对应表)
;

delete from 同业对应不上so单号_人工核对 where account_type = '在线支付' and goods_name = '88VIP购物卡月卡';
delete from 同业对应不上so单号_人工核对 where merchant_order_number = '202410031100300301560051961999' ;
delete from 同业对应不上so单号_人工核对 where merchant_order_number = '202410070021410528790041691064';
delete from 同业对应不上so单号_人工核对 where merchant_order_number = 'OTP240803212100002';
delete from 同业对应不上so单号_人工核对 where merchant_order_number in (select 商户订单号 from 1119_1120同业人工下单订单号核对);
delete from 同业对应不上so单号_人工核对 where account_type = '在线支付' and goods_name = '88VIP购物卡年卡';
delete from 同业对应不上so单号_人工核对 where remark like '保险理赔-退换货运费险理赔-订单号%';


drop table if exists ty_02;
create table ty_02 as 
select 
so订单号 so_order_id,
sum(income_amount + -abs(expenditure_amount)) transactionamount,
group_concat(distinct merchant_order_number separator'~') orderid from ty_01  where so订单号 is not null  group by so订单号;

insert into ty_02 select * from 麦腾_同业识货拼多多_成本结果表;
update ty_02 set so_order_id =substring(so_order_id,1,11) where length(so_order_id)>11;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 18  同业基础数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 18  同业基础数据 etl 失败   ">> ${errologs}   
    
fi











##########################################################################################################################
#中文描述：平台渠道账务数据 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST_2 -uroot -pPASS -D bigdata_mt -e "

--     杰之行走账表  需要人工导入
--     老卢          需要人工导入
--     澳美          需要人工导入
--     流苏          人工导入

-- 中盛 需要重新跑   （和大树一样不能 一天一天跑）






-- 西街
truncate table  xijie01;
insert into  xijie01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.xijie_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.xijie_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 乔乐
truncate table  qiaole01;
insert into  qiaole01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.qiaole_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.qiaole_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;

-- 第五季
truncate table  diwuji01;
insert into diwuji01 
select substring(线上订单号,1,11) so_order_id,group_concat(distinct 订单号 separator'~') orderid,sum(金额) transactionamount  from dugdb.diwuji_account_人工下载 where 线上订单号 like 'SO%' group by substring(线上订单号,1,11) ;


-- 名鞋库
truncate table  mingxieku01;
insert into mingxieku01 
select substring(orderid,1,11) so_order_id,group_concat(distinct remark separator'~') orderid,sum(transactionamount) transactionamount  from dugdb.mingxieku_account where orderid like 'SO%' group by orderid ;



-- 比恩
truncate table  bien01;
insert into bien01 
select substring(orderid,1,11) so_order_id,group_concat(distinct remark separator'~') orderid,sum(transactionamount) transactionamount  from dugdb.bien_account_人工下载 where orderid like 'SO%' group by substring(orderid,1,11) ;




-- 耐创
truncate table  naichuang01;
insert into  naichuang01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.naichuang_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.naichuang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 斌狗机舱
truncate table  bingoujicang01;
insert into bingoujicang01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.bingoujicang_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.bingoujicang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 突破运动
truncate table  tupoyundong01;
insert into tupoyundong01 
select b.so_order_id,group_concat(distinct a.订单号 separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select 订单号,sum(金额) transactionamount from dugdb.tupoyundong_account_人工下载  where 创建时间 >= '2026-01-01 00:00:00' group  by 订单号) a 
left join 
(select 订单号,substring(外部订单号,1,11) so_order_id from dugdb.tupoyundong_samp_order_人工下载) b 
on a.订单号=b.订单号 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 海玲轩

truncate table  hailingxuan01;
insert into hailingxuan01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.hailingxuan_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.hailingxuan_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 胡嘉兴
truncate table  hujiaxing01;
insert into hujiaxing01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.hujiaxing_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.hujiaxing_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 孤帆逐日 

truncate table  gufanzhuri01;
insert into gufanzhuri01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.gufanzhuri_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.gufanzhuri_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 清锐

truncate table  qingrui01;
insert into qingrui01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.qingrui_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.qingrui_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;






-- 雨歌

truncate table  yuge01;
insert into yuge01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.yuge_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.yuge_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;





-- 黑石

update dugdb.heishi_account_人工下载 set 订单=备注 where 订单 not like 'SO%';
update dugdb.heishi_account_人工下载 set 订单= substring(订单,1,11) where 订单 like 'SO%';


truncate table  heishi01;
insert into heishi01 
select 订单 so_order_id,group_concat(distinct 订单 separator'~') orderid,sum(收支) transactionamount from dugdb.heishi_account_人工下载  where 订单 like 'SO%' group by 订单;





-- 领衔
truncate table  lingxian01;
insert into lingxian01 
select substring(订单号,1,11) so_order_id,group_concat(distinct 订单号 separator'~') orderid,sum(支出金额) transactionamount from dugdb.lingxian_account_人工下载  where 订单号 like 'SO%' group by substring(订单号,1,11);



-- 橙子运动
truncate table  chengziyundong01;
insert into chengziyundong01 
select substring(线上订单号,1,11) so_order_id,group_concat(distinct 订单号 separator'~') orderid,sum(金额) transactionamount from dugdb.chengziyundong_account_人工下载  where 线上订单号 like 'SO%' group by substring(线上订单号,1,11);


-- 苇玖城
truncate table  weijiucheng01;
insert into weijiucheng01 
select substring(线上订单号,1,11) so_order_id,group_concat(distinct 订单号 separator'~') orderid,sum(金额) transactionamount from dugdb.weijiucheng_account_人工下载  where 线上订单号 like 'SO%' group by substring(线上订单号,1,11);





-- 全勇
update dugdb.quanyong_account set transactionamount = -abs(transactionamount) where type = '扣款';
update dugdb.quanyong_account set transactionamount = -abs(transactionamount) where type = '订单审核扣款';


update dugdb.quanyong_account a,dugdb.wangdiantong_refund b 
set a.orderid = b.平台订单号

where 
a.remark = b.推送单号 and  a.remark like 'RK%';




truncate table  quanyong01;
insert into quanyong01 
select 
substring(a.orderid,1,11) so_order_id,
group_concat(distinct substring(a.orderid,1,11) separator'~') orderid,
sum(a.transactionamount) transactionamount 
from 


(select orderid,transactionamount from dugdb.quanyong_account  union all 
select orderid,transactionamount from dugdb.quanyong_account_20251026退款) a 


where a.orderid like 'SO%' group by substring(a.orderid,1,11);





-- 威海斯博兹

update dugdb.weihaisibozi_account set transactionamount = -abs(transactionamount) where type like '%扣款%';

update dugdb.weihaisibozi_account a,dugdb.wangdiantong_refund b 
set a.orderid = b.平台订单号

where 
a.remark = b.推送单号 and  a.remark like 'RK%';


update dugdb.weihaisibozi_account a,dugdb.weihaisibozi_samp_order b 
set a.orderid = substring(b.externalorderno,1,11)

where 
a.orderid = b.orderno and  a.orderid not like 'SO%';


truncate table  weihaisibozi01;
insert into weihaisibozi01 
select 
substring(orderid,1,11) so_order_id,
group_concat(distinct substring(orderid,1,11) separator'~') orderid,
sum(transactionamount) transactionamount 
from 
dugdb.weihaisibozi_account  
where orderid like 'SO%' group by substring(orderid,1,11);






-- 全勇 退款是批量退  2025-08-04  批量退款  25笔  3280.1 元 已经核实没问题  
-- 现处理方法  将已经退款订单从 成本统计中删除
update qiaole01 set transactionamount = 0  where so_order_id in (
'SO008528650',
'SO008458560',
'SO008457613',
'SO008460915',
'SO008527269',
'SO008541801',
'SO008544343',
'SO008546307',
'SO008466203',
'SO008529608',
'SO008458585',
'SO008544442',
'SO008485538',
'SO008549429',
'SO008496744',
'SO008486836',
'SO008496979',
'SO008461398',
'SO008476727',
'SO008482504',
'SO008527660',
'SO008523296',
'SO008522042',
'SO008490265',
'SO008547266');


-- 趣淘


update dugdb.qutao_account set transactionamount = -abs(transactionamount) where type like '%扣款%';

update dugdb.qutao_account a,dugdb.wangdiantong_refund b 
set a.orderid = b.平台订单号

where 
a.remark = b.推送单号 and  a.remark like 'RK%';




truncate table  qutao01;
insert into qutao01 
select 
substring(orderid,1,11) so_order_id,
group_concat(distinct substring(orderid,1,11) separator'~') orderid,
sum(transactionamount) transactionamount 
from 
dugdb.qutao_account  
where orderid like 'SO%' group by substring(orderid,1,11);




-- 凌齿龙

truncate table  lingchilong01;
insert into lingchilong01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.lingchilong_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.lingchilong_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;





-- 得力聚川

truncate table  delijuchuan01;
insert into delijuchuan01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.delijuchuan_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.delijuchuan_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 兴悦奥特莱斯

truncate table  xingyueaotelaisi01;
insert into xingyueaotelaisi01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.xingyueoutlets_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.xingyueoutlets_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 公司共鞋出货订单   麦光共鞋出库项目  在线表格维护 现在是人工导入更新
truncate table  gongxie_myjochuku01;
insert into gongxie_myjochuku01 
select SO单号 so_order_id,'gongxiechuku' orderid,sum(-abs(成本价)) transactionamount from dugdb.公司共鞋出货订单 group by SO单号;








-- 激想团购
truncate table  jixiangtuangou01;
insert into jixiangtuangou01 
select 外部订单号 so_order_id,'test' orderid,-abs(round(sum((订单支付金额总 - 退款金额)),2)) transactionamount from dugdb.激想团购订单 group by 外部订单号;



/*
-- 领衔

truncate table  lingxian01;
insert into  lingxian01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.lingxian_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.lingxian_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



*/



--  味亦

truncate table  weiyi01;
insert into  weiyi01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 

(
select xx.orderid,sum(xx.transactionamount) transactionamount from 

(select orderid,transactionamount from dugdb.weiyi_account  where time >= '2026-01-01 00:00:00'  union all select 订单号,退货款金额 from dugdb.weiyi_account_平台批量退订单退款明细表0918_不能删除) xx  



group  by xx.orderid) a 



left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.weiyi_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




--  速冠
truncate table  suguan01;
insert into  suguan01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.suguan_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.suguan_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 激想


update dugdb.jixiang_account set orderid = SUBSTRING_INDEX(remark,':',-1) where remark like '%:T202%';

truncate table  jixiang01;
insert into  jixiang01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.jixiang_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.jixiang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;





-- 杰之宁
truncate table  jiezhining01;
insert into  jiezhining01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.jiezhining_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.jiezhining_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;





-- 格林岛
truncate table  gelindao01;
insert into  gelindao01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.gelindao_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.gelindao_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 立臻
truncate table  lizhen01;
insert into  lizhen01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.lizhen_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.lizhen_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 亿网
delete from dugdb.yiwang_account where balance is null;


truncate table  yiwang01;
insert into  yiwang01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.yiwang_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.yiwang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 成目商开发
truncate table  chengmushang01;
insert into  chengmushang01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select substring_index(orderid,'-',1) orderid,sum(transactionamount) transactionamount from dugdb.chengmushang_account  where time >= '2026-01-01 00:00:00' group  by substring_index(orderid,'-',1)) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.chengmushang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;

-- 中盛开发

truncate table  zhongsheng01;
insert into  zhongsheng01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select substring_index(orderid,'-',1) orderid,sum(transactionamount) transactionamount from dugdb.zhongsheng_account  where time >= '2026-01-01 00:00:00' and orderid not like '%EP%' 
group  by substring_index(orderid,'-',1)) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.zhongsheng_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;






-- 澳美平台 

truncate table  aomei01;
insert into  aomei01 

select 
aa.so_order_id,
'' orderid,
SUM(aa.transactionamount) transactionamount 
from 
(
 
select 
substring(so_order_id,1,11) so_order_id,
refund_amount transactionamount 
from dugdb.aomei_refund

union all 
select 
substring(orderid,1,11) so_order_id,
transactionamount 
from dugdb.aomei_account where type = '充值' 

union all 

select 
substring(orderid,1,11) so_order_id,
-abs(transactionamount) transactionamount  
from dugdb.aomei_account where type = '扣款' 

union all select substring(so单号,1,11) so_order_id,-abs(value) transactionamount  from dugdb.澳美_退款表_澳美九月十月退一单充值2单问题 


) aa 
group by aa.so_order_id;




-- 流苏账务

delete from dugdb.liusu_account where balance is null;
truncate table  liusu01;
insert into  liusu01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.liusu_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.liusu_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;







-- 宝原
truncate table  baoyuan01;
insert into  baoyuan01 
select 
m.so_order_id,
group_concat(distinct ordercode separator'~') orderid,
sum(m.costprice) transactionAmount 

from 

(
select 
substring(replace(a.outid,'DSP034',''),1,11) so_order_id,
a.ordercode,
-abs(b.paymentamount)+if(c.totalprice is null,0,c.totalprice) costprice,
-abs(b.paymentamount) paymentamount,
if(c.totalprice is null,0,c.totalprice) totalprice 
from dugdb.baoyuan_samp_delivery a 
left join dugdb.baoyuan_samp_order b 
on a.ordercode=b.orderno 

left join 
(select ordercode,totalprice from dugdb.baoyuan_samp_rejection where releoutorderid is not null) c on a.ordercode=c.ordercode 

where  a.status = 5 and a.ordertime > '2026-01-01 00:00:00' 
) m 

group by m.so_order_id 
order by sum(m.costprice);



update baoyuan01 set transactionamount = 0 where transactionamount is null;


/*
人工下载计算
truncate table  baoyuan01;
insert into  baoyuan01 
select 
m.so_order_id,
group_concat(distinct ordercode separator'~') orderid,
sum(m.costprice) transactionAmount 

from 

(
select substring(replace(a.outid,'DSP034',''),1,11) so_order_id,a.ordercode,-abs(a.costPrice) costprice from dugdb.baoyuan_samp_delivery_copy1 a  where  a.status != '已取消' and a.ordertime > '2026-01-01 00:00:00'  union all 
select substring(replace(releOutOrderId,'DSP034',''),1,11) so_order_id,ordercode,refundfee costprice from dugdb.baoyuan_samp_rejection_copy1   where  status != '已取消' and releOutOrderId is not null
) m 

group by m.so_order_id 
order by sum(m.costprice);

delete from bigdata_mt.baoyuan01 where transactionAmount is null;

delete from bigdata_mt.baoyuan01 where transactionAmount>=0;
*/






-- 育泰开发

update dugdb.yutai_account a ,(select refundno,orderid from dugdb.yutai_samp_refund) b 
set a.orderid=b.orderid 
where a.orderid=b.refundno ;

truncate table  yutai01;
insert into  yutai01 

select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 

(

select x.orderid,sum(x.transactionamount) transactionamount from 

(select orderid,transactionamount from dugdb.yutai_account  where time >= '2026-01-01 00:00:00' and orderid not like 'QDYE%' ) x 


group  by orderid

) a 




left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.yutai_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 一尧
truncate table  yiyao01;
insert into  yiyao01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.yiyao_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.yiyao_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 畅跑
update dugdb.changpao_account set transactionamount=abs(transactionamount) where type = '购物取消退款';
update dugdb.changpao_account set transactionamount=-abs(transactionamount) where type = '购物支出';
update dugdb.changpao_account set transactionamount=abs(transactionamount) where type = '售后退款';
update dugdb.changpao_account set orderid=replace(substring_index(remark,',',1),'订单','') where remark like '%售后退款%';

truncate table  changpao01;
insert into  changpao01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select replace(orderid,'订单','') orderid,sum(transactionamount) transactionamount from dugdb.changpao_account  
where time >= '2026-01-01 00:00:00' and orderid !='' group  by replace(orderid,'订单','')) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.changpao_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 瑞动

truncate table  ruidong01;
insert into  ruidong01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.ruidong_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.ruidong_samp_order ) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 国域

update dugdb.guoyu_account set orderid = replace(orderid,'A','') where orderid like '%A';
update dugdb.guoyu_samp_order set orderno = replace(orderno,'A','') where orderno like '%A';

truncate table  guoyu01;
insert into  guoyu01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.guoyu_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.guoyu_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 互动
truncate table  hudong01;
insert into  hudong01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(
select x.orderid,sum(x.transactionamount) transactionamount from 
(select orderid,transactionamount from dugdb.hudong_account  where time >= '2026-01-01 00:00:00'  union all 
select 渠道订单号 orderid,退款金额 transactionamount from dugdb.hudong_批量退订单明细_财务财务更新_不删 ) x 

group  by x.orderid

) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.hudong_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 非凡

truncate table  feifan01;

update dugdb.feifan_account set orderid=replace(orderid,'-1','')  where orderid like '%-1%' and orderid not like 'A%';


insert into  feifan01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount 
from dugdb.feifan_account  where time >= '2026-01-01 00:00:00' and type != '充值' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.feifan_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id ;



-- 百宏
truncate table  baihong01;
insert into  baihong01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.baihong_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.baihong_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 登腾
truncate table  dengteng01;
insert into  dengteng01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.dengteng_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.dengteng_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



insert into  dengteng01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.dengtengadidas_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.dengtengadidas_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 聚美优特

update dugdb.juyoumeite_account set orderid= substring(remark,1,16) where orderid = '' and type != '充值';
update dugdb.juyoumeite_samp_order set externalorderno=replace(externalOrderNo,' ','') where externalOrderNo like '% %';
truncate table  juyoumeite01;
insert into  juyoumeite01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.juyoumeite_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.juyoumeite_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 易商
update dugdb.yishang_account a,dugdb.yishang_samp_refund b set a.orderid=b.orderid where a.orderid=b.refundno and a.orderid like 'xsth%';
truncate table  yishang01;
insert into  yishang01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.yishang_account  where time >= '2026-01-01 00:00:00' and remark not like '%汇款方式%' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.yishang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 五哲
/*
update dugdb.wuzhe_account a ,dugdb.wuzhe_samp_refund b set a.orderid=b.orderid where a.orderid=b.refundno and a.remark='售后退款';
update dugdb.wuzhe_account_新系统手动更新 a ,dugdb.wuzhe_samp_refund_新系统手动导出_导出退单 b set a.单据编号=b.原始订单号 where a.单据编号=b.原始退单号 and a.来源单据名称='电商零售退单';

truncate table  wuzhe01;
insert into  wuzhe01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(
select x.orderid,sum(x.transactionamount) transactionamount from 
(
select orderid,transactionamount from dugdb.wuzhe_account  where time >= '2026-01-01 00:00:00'  union all   select 单据编号 orderid,资金余额 transactionamount from dugdb.wuzhe_account_新系统手动更新

) x group  by x.orderid
) a 

left join 
(
select distinct y.orderno,substring(y.externalorderno,1,11) so_order_id from (select orderno,externalorderno from dugdb.wuzhe_samp_order union all select 订单号 orderno,外部系统订单号 externalorderno from dugdb.wuzhe_samp_order_新系统手动更新

) y 
) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;

*/



update  dugdb.wuzhe_account_新系统手动更新 a ,(select  E3订单号,E3售后单号 from dugdb.wuzhe_samp_refund_新系统手动导出_导出退单) b 
set a.关联单号 = b.E3订单号 
where a.关联单号 = b.E3售后单号 and a.外部订单号 not like 'SO%' and a.关联单号 like '%R%';


update  dugdb.wuzhe_account_新系统手动更新 a ,(select  e3单号,外部平台单号 from dugdb.wuzhe_samp_order_新系统手动更新) b 
set a.外部订单号 = b.外部平台单号 
where a.关联单号 = b.e3单号 and a.外部订单号 not like 'SO%';


truncate table  wuzhe01;
insert into  wuzhe01 
select substring(外部订单号,1,11) so_order_id,group_concat(distinct 关联单号 separator'~') orderid,sum(资金占用额度) transactionamount  
from dugdb.wuzhe_account_新系统手动更新 
where 外部订单号 is not null 
group by substring(外部订单号,1,11);




-- 法雅

update dugdb.faya_account a,dugdb.faya_samp_refund b 
set a.orderid=b.orderid 
where a.orderid=b.refundNo and a.type='退单加款' and a.orderid like '9%';

truncate table  faya01;
insert into  faya01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.faya_account  where time >= '2026-01-01 00:00:00' and type !='充值'  and type !='调整减' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.faya_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 迈盛悦和

truncate table  maishengyuehe01;
insert into  maishengyuehe01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.maishengyuehe_account  where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.maishengyuehe_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 大树
/*

update   dugdb.dashu_account 
set transactionAmount = -ABS(transactionAmount) 
where type = '分销商B2C订单' and remark = '订单';

update   dugdb.dashu_account 
set transactionAmount = -ABS(transactionAmount) 
where type = '分销直营门店O2O销售订单' and remark = '订单';

update   dugdb.dashu_account 
set transactionAmount = ABS(transactionAmount) 
where type = 'B2C退货订单' and remark = '订单';


truncate table  dashu01;
insert into  dashu01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select replace(orderid,'-JS','') orderid,sum(transactionamount) transactionamount from dugdb.dashu_account  
where time >= '2026-01-01 00:00:00' and orderid like 'EO%' group  by replace(orderid,'-JS','')) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.dashu_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;
*/


-- 人工下载版本


-- update dugdb.大树尚玄_samp_order set 源单号 = replace(源单号,"'",'') where 源单号 like "%'%";
-- update dugdb.大树时禾美_samp_order set 源单号 = replace(源单号,"'",'') where 源单号 like "%'%";
-- update dugdb.大树尚玄_account set 单号 = replace(单号,"-JS",'') where 单号 like "%-JS%";
-- update dugdb.大树时禾美_account set 单号 = replace(单号,"-JS",'') where 单号 like "%-JS%";




truncate table  dashu01;
insert into  dashu01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 

(
select xx.orderid,sum(xx.transactionamount) transactionamount from 
(
select 单号 orderid,冲减金额 * -1 transactionamount from dugdb.大树时禾美_account where 单据日期 like '2025%' union all 
 select 单号,冲减金额 * -1 transactionamount from dugdb.大树尚玄_account where 单据日期 like '2025%' union all 
 select 单号 orderid,冲减金额 * -1 transactionamount from dugdb.大树时禾美_account where 单据日期 like '2026%' union all 
 select 单号,冲减金额 * -1 transactionamount from dugdb.大树尚玄_account where 单据日期 like '2026%' 


) xx  group by xx.orderid
) a 

left join 
(
select  distinct yy.orderno,substring(yy.externalorderno,1,11) so_order_id from 
(
select 订单号 orderno,substring(源单号,1,11) externalorderno from dugdb.大树时禾美_samp_order union all 
select 订单号 orderno,substring(源单号,1,11) externalorderno from dugdb.大树尚玄_samp_order union all 
select orderno,substring(externalorderno,1,11) externalorderno from dugdb.dashu_samp_order 
) yy 
) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;













-- 杰之行
update dugdb.jiezhixing_account a,dugdb.jiezhixing_zouzhang b 
set a.orderid=b.orderid  
where a.orderid=b.zouzhangdanhao  and a.type='走账的扣款金额' and a.orderid like 'zz2%';

truncate table  jiezhixing01;
insert into  jiezhixing01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.jiezhixing_account where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.jiezhixing_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 联合尚品

truncate table  lianheshangpin01;
insert into  lianheshangpin01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.lianheshangpin_account where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.lianheshangpin_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;





-- 尚动


update dugdb.shangdong_account set orderid = replace(replace(remark,'订单',''),',售后退款','') where orderid like 'R%' and remark like '%售后退款%' ;

update  dugdb.shangdong_account set transactionAmount = -ABS(transactionAmount) where type = '购物支出';
truncate table  shangdong01;
insert into  shangdong01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.shangdong_account where time >= '2026-01-01 00:00:00' and type !='充值' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.shangdong_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



-- 宝胜

truncate table  baosheng01;
insert into  baosheng01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 


(
select xx.orderid,sum(xx.transactionamount) transactionamount from 

(
select orderid,transactionamount from dugdb.baosheng_account where time >= '2026-01-01 00:00:00' and type != '续费' and orderid is not null union all 
select orderid,transactionamount from dugdb.baoweitwo_account where time >= '2026-01-01 00:00:00' and orderid is not null 

) xx
group  by xx.orderid
) a 


left join 
(
select orderno,substring(externalorderno,1,11) so_order_id from dugdb.baosheng_samp_order union all 
select orderno,substring(externalorderno,1,11) so_order_id from dugdb.baoweitwo_samp_order

) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



--  劲浪

truncate table  jinlang01;
insert into  jinlang01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.jinlang_account where time >= '2026-01-01 00:00:00' and type !='续费'  group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.jinlang_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;



--  凡兮

truncate table  fanxi01;
insert into  fanxi01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.fanxi_account where time >= '2026-01-01 00:00:00' and type !='财务续费'  group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.fanxi_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


-- 酷锐
truncate table  kurui01;
insert into  kurui01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.ikoori_account where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.ikoori_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null and b.so_order_id like '%so%'
group by b.so_order_id 
;







-- 杰斯拓


update dugdb.jiesituo_account_人工下载 set 订单编号 = replace(订单编号,'\'','');
update dugdb.jiesituo_account_人工下载 set 到账金额 =0 where 到账金额 is null;
update dugdb.jiesituo_account_人工下载 set 支出金额 =0 where 支出金额 is null;



truncate table  jiesituo01;
insert into  jiesituo01 

select xx.so_order_id,group_concat(distinct xx.orderid separator'~') orderid,sum(xx.transactionamount) transactionamount  from 


(
select so_order_id,a.orderid orderid,a.transactionamount transactionamount 
from 

(
select m.orderid,round(sum(m.transactionamount),2) transactionamount from 

(select orderid,transactionamount from  dugdb.jiesituo_account where time >= '2026-01-01 00:00:00' and type !='续费' union all 


select 平台订单编号 orderid,金额 transactionamount  from dugdb.jiesituo_account_20250106集中退款_绝对不能删除 union all 

select 订单编号 orderid,到账金额-支出金额  transactionamount from dugdb.jiesituo_account_人工下载 where  订单编号 is not null 

) m 



group  by m.orderid


) a 


left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.jiesituo_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 


union all select 订单号 so_order_id,'' orderid,退款金额 transactionamount from dugdb.杰斯拓平台_老系统ETL调度订单20251030前订单回款 
) 
xx 




group by xx.so_order_id 
;





-- 文石
truncate table  wenshi01;
insert into  wenshi01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.wenshi_account where time >= '2026-01-01 00:00:00' and type !='充值' and orderid != '' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.wenshi_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null and b.so_order_id like '%so%' 
group by b.so_order_id 
;




-- 天马


-- 天马平台orderid 是 null   天马单号再 remark 中

update dugdb.tianma_account set orderid = substring(remark,1,9) 
where 
time >= '2024-01-01 00:00:00'  
and orderid is null 
and remark not like '%充值%' 
and remark not like '%TM转出 扣除金额%'
and remark not like '%镇江仓%'
and remark not like '%山东QD%' 
and remark like '99%';

update dugdb.tianma_account set orderid = substring(remark,1,9) 
where 
time >= '2024-01-01 00:00:00'  
and orderid is null 
and remark not like '%充值%' 
and remark not like '%TM转出 扣除金额%'
and remark not like '%镇江仓%'
and remark not like '%山东QD%' 
and remark like '98%';

update dugdb.tianma_account set orderid = substring(remark,1,10) 
where 
time >= '2024-01-01 00:00:00'  
and orderid is null 
and remark not like '%充值%' 
and remark not like '%TM转出 扣除金额%'
and remark not like '%镇江仓%'
and remark not like '%山东QD%' 
and remark like '10%';



truncate table  tianma01;
insert into  tianma01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.tianma_account where time >= '2026-01-01 00:00:00' and type != '充值'  group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.tianma_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;




-- 宝福来
truncate table   baofulai01;
insert into  baofulai01 
select b.so_order_id,group_concat(distinct a.orderid separator'~') orderid,sum(a.transactionamount) transactionamount 
from 
(select orderid,sum(transactionamount) transactionamount from dugdb.baofulai_account where time >= '2026-01-01 00:00:00' group  by orderid) a 
left join 
(select orderno,substring(externalorderno,1,11) so_order_id from dugdb.baofulai_samp_order) b 
on a.orderid=b.orderno 
where b.so_order_id is not null 
group by b.so_order_id 
;


insert into  baofulai01 
select  
substring(平台订单号,1,11) so_order_id,
group_concat(distinct 单号) orderid,
sum(金额) transactionamount 
from  dugdb.共鞋erp账单表 
where 平台订单号 like 'SO%' 
group by substring(平台订单号,1,11);



"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 平台成本计算 成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 平台成本计算 失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysqldump --column-statistics=0 --set-gtid-pPASS -hRDS_HOST_2 -uroot -pPASS --databases bigdata_mt > /data/exchange/bigdata_mt.sql

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -e "drop database if exists bigdata_mt;"

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS  rugen < /data/exchange/bigdata_mt.sql


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 平台数据备份成功  etl  恢复 成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 平台数据备份成功  etl  恢复 失败   ">> ${errologs}   
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -e "
use bigdata_mt;

delete from bigdata_mt.lianheshangpin01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.tianma01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.yiyao01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.yutai01 where  length(orderid) > 150;

delete from bigdata_mt.shangdong01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.tianma01 where so_order_id not like '%SO%' and so_order_id not like '%CO%';
delete from bigdata_mt.baihong01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.fanxi01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.juyoumeite01 where so_order_id  like '%CO%' and length(orderid) > 100;
delete from bigdata_mt.baosheng01 where so_order_id  like '%CO%' and length(orderid) > 100;

delete from bigdata_mt.wuzhe01 where so_order_id not like '%SO%' and so_order_id not like '%CO%';
delete from bigdata_mt.feifan01 where so_order_id not like '%SO%' and so_order_id not like '%CO%';

ALTER TABLE bigdata_mt.xijie01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.xijie01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.qiaole01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.qiaole01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.suguan01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.suguan01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.weiyi01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.weiyi01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.jixiang01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jixiang01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;


ALTER TABLE bigdata_mt.jiezhining01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jiezhining01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.gelindao01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.gelindao01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;

ALTER TABLE bigdata_mt.lizhen01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.lizhen01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;


ALTER TABLE bigdata_mt.yiwang01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yiwang01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;



ALTER TABLE bigdata_mt.tiaohuo MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.tiaohuo MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;


ALTER TABLE bigdata_mt.zhongsheng01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.zhongsheng01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;


ALTER TABLE bigdata_mt.chengmushang01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.chengmushang01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

ALTER TABLE bigdata_mt.baoyuan01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
-- ALTER TABLE bigdata_mt.baoyuan01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

ALTER TABLE bigdata_mt.liusu01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.liusu01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

ALTER TABLE bigdata_mt.aomei01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.aomei01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

ALTER TABLE bigdata_mt.laolu01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.laolu01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;


ALTER TABLE bigdata_mt.baihong01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.baofulai01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.baosheng01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.changpao01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.dashu01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.dengteng01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.fanxi01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.faya01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.feifan01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.guoyu01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.hudong01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jiesituo01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jiezhixing01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jinlang01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.juyoumeite01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.kurui01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.lianheshangpin01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.maishengyuehe01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.ruidong01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.shangdong01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.tianma01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.wenshi01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.wuzhe01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yishang01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yiyao01 MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yutai01  MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.baihong01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.baofulai01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.baosheng01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.changpao01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.dashu01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.dengteng01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.fanxi01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.faya01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.feifan01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.guoyu01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.hudong01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jiesituo01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jiezhixing01 MODIFY orderid VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.jinlang01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.juyoumeite01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.kurui01 MODIFY orderid VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.lianheshangpin01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.maishengyuehe01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.ruidong01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.shangdong01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.tianma01 MODIFY orderid VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.wenshi01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.wuzhe01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yishang01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yiyao01 MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.yutai01  MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE bigdata_mt.diwuji01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.mingxieku01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.bien01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.naichuang01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.bingoujicang01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.tupoyundong01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.hailingxuan01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.hujiaxing01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.gufanzhuri01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.qingrui01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.yuge01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.heishi01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.lingxian01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.chengziyundong01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.weijiucheng01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.quanyong01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.weihaisibozi01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.qutao01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.lingchilong01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.delijuchuan01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.xingyueaotelaisi01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.gongxie_myjochuku01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.jixiangtuangou01   MODIFY orderid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE rugen.local_warehouse01  MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE rugen.local_warehouse01  MODIFY mjBarcode VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;


ALTER TABLE bigdata_mt.diwuji01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.mingxieku01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.bien01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.naichuang01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.bingoujicang01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.tupoyundong01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.hailingxuan01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.hujiaxing01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.gufanzhuri01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.qingrui01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.yuge01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.heishi01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.lingxian01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.chengziyundong01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.weijiucheng01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.quanyong01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.weihaisibozi01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.qutao01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.lingchilong01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.delijuchuan01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.xingyueaotelaisi01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.gongxie_myjochuku01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 
ALTER TABLE bigdata_mt.jixiangtuangou01   MODIFY so_order_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; 




ALTER TABLE dengteng01 ADD INDEX idx (so_order_id);
ALTER TABLE fanxi01 ADD INDEX idx (so_order_id);
ALTER TABLE faya01 ADD INDEX idx (so_order_id);
ALTER TABLE feifan01 ADD INDEX idx (so_order_id);
ALTER TABLE gelindao01 ADD INDEX idx (so_order_id);
ALTER TABLE guoyu01 ADD INDEX idx (so_order_id);
ALTER TABLE hudong01 ADD INDEX idx (so_order_id);
ALTER TABLE jiesituo01 ADD INDEX idx (so_order_id);
ALTER TABLE jiezhining01 ADD INDEX idx (so_order_id);
ALTER TABLE jiezhixing01 ADD INDEX idx (so_order_id);
ALTER TABLE jiezhixing02 ADD INDEX idx (orderid);
ALTER TABLE jinlang01 ADD INDEX idx (so_order_id);
ALTER TABLE jixiang01 ADD INDEX idx (so_order_id);
ALTER TABLE juyoumeite01 ADD INDEX idx (so_order_id);
ALTER TABLE kurui01 ADD INDEX idx (so_order_id);
ALTER TABLE laolu01 ADD INDEX idx (so_order_id);
ALTER TABLE lianheshangpin01 ADD INDEX idx (so_order_id);
ALTER TABLE lingxian01 ADD INDEX idx (so_order_id);
ALTER TABLE liusu01 ADD INDEX idx (so_order_id);
ALTER TABLE lizhen01 ADD INDEX idx (so_order_id);
ALTER TABLE maishengyuehe01 ADD INDEX idx (so_order_id);
ALTER TABLE qiaole01 ADD INDEX idx (so_order_id);
ALTER TABLE ruidong01 ADD INDEX idx (so_order_id);
ALTER TABLE shangdong01 ADD INDEX idx (so_order_id);
ALTER TABLE suguan01 ADD INDEX idx (so_order_id);
ALTER TABLE tianma01 ADD INDEX idx (so_order_id);
ALTER TABLE tiaohuo ADD INDEX idx (so_order_id);
ALTER TABLE weiyi01 ADD INDEX idx (so_order_id);
ALTER TABLE wenshi01 ADD INDEX idx (so_order_id);
ALTER TABLE wuzhe01 ADD INDEX idx (so_order_id);
ALTER TABLE xijie01 ADD INDEX idx (so_order_id);
ALTER TABLE yishang01 ADD INDEX idx (so_order_id);
ALTER TABLE yiwang01 ADD INDEX idx (so_order_id);
ALTER TABLE yiyao01 ADD INDEX idx (so_order_id);
ALTER TABLE yutai01 ADD INDEX idx (so_order_id);
ALTER TABLE zhongsheng01  ADD INDEX idx (so_order_id);
ALTER TABLE diwuji01   ADD INDEX idx (so_order_id);
ALTER TABLE mingxieku01   ADD INDEX idx (so_order_id);
ALTER TABLE bien01   ADD INDEX idx (so_order_id);
ALTER TABLE naichuang01   ADD INDEX idx (so_order_id);
ALTER TABLE bingoujicang01   ADD INDEX idx (so_order_id);
ALTER TABLE tupoyundong01   ADD INDEX idx (so_order_id);
ALTER TABLE hailingxuan01   ADD INDEX idx (so_order_id);
ALTER TABLE hujiaxing01   ADD INDEX idx (so_order_id);
ALTER TABLE gufanzhuri01   ADD INDEX idx (so_order_id);
ALTER TABLE qingrui01   ADD INDEX idx (so_order_id);
ALTER TABLE yuge01   ADD INDEX idx (so_order_id);
ALTER TABLE heishi01   ADD INDEX idx (so_order_id);
ALTER TABLE chengziyundong01   ADD INDEX idx (so_order_id);
ALTER TABLE weijiucheng01   ADD INDEX idx (so_order_id);
ALTER TABLE quanyong01   ADD INDEX idx (so_order_id);
ALTER TABLE weihaisibozi01   ADD INDEX idx (so_order_id);
ALTER TABLE qutao01   ADD INDEX idx (so_order_id);
ALTER TABLE lingchilong01   ADD INDEX idx (so_order_id);
ALTER TABLE delijuchuan01   ADD INDEX idx (so_order_id);
ALTER TABLE xingyueaotelaisi01   ADD INDEX idx (so_order_id);
ALTER TABLE gongxie_myjochuku01   ADD INDEX idx (so_order_id);
ALTER TABLE jixiangtuangou01   ADD INDEX idx (so_order_id);

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 字符集转换  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 字符集转换 失败   ">> ${errologs}   
    
fi







## 在此处增加 换货逻辑
## 换货给买家发出  

## 换货逻辑暂时先不加入到ETL调度中 等业务弄好了在加

## 现在只是走这个流程   result_sale_order04 先不加这个逻辑
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "

drop table if exists 换货订单明细 ;
create table 换货订单明细 as 
SELECT
    co_name ,
    so_name,receive_name,
    express_company_code ,
    express_number ,
    platform ,
    ps_gx_order_no ,
    post_fee ,
    amount_total ,
    pay_amount ,
    co_status ,
    ps_gx_order_state ,
        FROM_UNIXTIME(floor(created_time/1000)) created_time,
    remark,
        replace(SUBSTRING_INDEX(SUBSTRING_INDEX(remark,' ',1),':',-1),'换货','') source  
FROM
     erp_db.samp_cut_goods_order 
WHERE
    created_time  >= 1767196800000   and co_status != 'CANCEL' and so_name like 'SO%' and ps_gx_order_state not like '%无货%';

    "


mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select  so_name,sum(amount_total) costprice_huanhuo from 换货订单明细  where so_name is not null group by so_name; ">/data/exchange/换货订单明细.txt



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  换货订单明细;load data local infile '/data/exchange/换货订单明细.txt' into table 换货订单明细 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

insert into bigdata_mt.tiaohuo 

select so_name,'切货_换货',costprice_huanhuo from rugen.换货订单明细;


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  换货处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 换货处理 失败   ">> ${errologs}   
    
fi







##########################################################################################################################
#中文描述：ERP售后单 基础表 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;


-- 天猫 淘宝 售后单 

drop table if exists result_sale_order_after_sales_tianmao_taobao;
create table result_sale_order_after_sales_tianmao_taobao as 
select 
a.id as_refund_line_id, 
FROM_UNIXTIME(floor(b.init_time/1000)) date_time,
b.total_amount, -- 总退款金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号

case 
    when b.refund_status='WAIT_SELLER_AGREE' then '退款待处理' 
    when b.refund_status='WAIT_BUYER_RETURN_GOODS' then '待买家发货' 
    when b.refund_status='WAIT_SELLER_CONFIRM_GOODS' then '待商家收货' 
    when b.refund_status='SELLER_REFUSE_BUYER' then '商家已拒绝' 
    when b.refund_status='CLOSED' then '退款关闭'    
    when b.refund_status='FINISH' then '退款完结' 
    when b.refund_status='UNDERWAY' then '进行中的订单' 
    when b.refund_status='WAITING_DEAL' then '退款待处理' 
    when b.refund_status='WAITING_BUYER_SEND' then '待买家发货' 
    when b.refund_status='WAITING_BUSINE_RECEIVED' then '待商家收货'    
    when b.refund_status='SUCCESS' then '退款成功' 
    end    refund_status, -- 售后状态

case 
    when a.good_status='BUYER_NOT_RECEIVED' then '仅退款' 
    when a.good_status='BUYER_RECEIVED' then '退货退款' 
    when a.good_status='BUYER_RETURNED_GOODS' then '退货退款' 
    end good_status,-- 退款类型

a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.init_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
case when b.business_type=1 then '未发秒退' else '' end weifamiaotui  

from erp_db.as_refund_info_line a 
left join  erp_db.as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
-- where FROM_UNIXTIME(floor(b.init_time/1000)) >'2026-01-01 00:00:00' 
where b.init_time >= 1767196800000 
;


-- 买家退回到本地仓快递单号 包括 同业
drop table if exists zhuanji_tianmao_taobao_to_local ;
create table zhuanji_tianmao_taobao_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_tianmao_taobao a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=1) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;



-- 获取转寄渠道快递单号
drop table if exists zhuanji_tianmao_taobao_to_qudao ;
create table zhuanji_tianmao_taobao_to_qudao as 
select * from (


select a.name,b.express_number_to_samp from  result_sale_order_after_sales_tianmao_taobao a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=1) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  天猫 淘宝 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 天猫 淘宝 售后单处理 失败   ">> ${errologs}   
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;




-- 抖店 ERP售后单

drop table if exists result_sale_order_after_sales_doudian;
create table result_sale_order_after_sales_doudian as 
select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.init_time/1000)) date_time,
b.total_amount, --  总退款金额
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
b.after_sale_type_text good_status,-- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.init_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.after_sale_type=2 then '未发秒退' else '' end weifamiaotui 

from erp_db.doudian_as_refund_info_line a 
left join  erp_db.doudian_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
-- where FROM_UNIXTIME(floor(b.init_time/1000)) >'2026-01-01 00:00:00' 
where b.init_time >= 1767196800000 
;


-- 买家退回到本地仓快递单号 包括 同业
drop table if exists zhuanji_doudian_to_local ;
create table zhuanji_doudian_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_doudian a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=3) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;



drop table if exists zhuanji_doudian_to_qudao ;
create table zhuanji_doudian_to_qudao as 
select * from (


select a.name,b.express_number_to_samp from  result_sale_order_after_sales_doudian a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=3) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;



"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  抖店 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 抖店 售后单处理 失败   ">> ${errologs}   
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;


-- 拼多多ERP售后单


drop table if exists result_sale_order_after_sales_pdd;
create table result_sale_order_after_sales_pdd as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.init_time/1000)) date_time,
b.refund_amount total_amount, --  总退款金额
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.refund_status, -- 售后状态
b.refund_pdd_type good_status,-- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.init_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 b.speed_refund_flag weifamiaotui 

from erp_db.pdd_as_refund_info_line a 
left join  erp_db.pdd_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
-- where FROM_UNIXTIME(floor(b.init_time/1000)) >'2026-01-01 00:00:00' 
where b.init_time >= 1767196800000 
;




-- 买家退回到本地仓快递单号 包括 同业
drop table if exists zhuanji_pdd_to_local ;
create table zhuanji_pdd_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_pdd a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=2) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_pdd_to_qudao ;
create table zhuanji_pdd_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_pdd a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=2) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  拼多多 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 拼多多 售后单处理 失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- 京东ERP售后单

drop table if exists result_sale_order_after_sales_jd;
create table result_sale_order_after_sales_jd as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount, -- 退款总金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
case 
    when b.after_sale_type=0 then '退货退款' 
    when b.after_sale_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.zero_response = 1 then '未发秒退' else '' end weifamiaotui 
 
from erp_db.jingdong_as_refund_info_line a 
left join  erp_db.jingdong_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;

drop table if exists zhuanji_jd_to_local ;
create table zhuanji_jd_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_jd a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=4) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;



drop table if exists zhuanji_jd_to_qudao ;
create table zhuanji_jd_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_jd a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=4) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;


"


mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- 京东到家ERP售后单

drop table if exists result_sale_order_after_sales_jddj;
create table result_sale_order_after_sales_jddj as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount, -- 退款总金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
case 
    when b.after_sale_type=0 then '退货退款' 
    when b.after_sale_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.zero_response = 1 then '未发秒退' else '' end weifamiaotui 

from erp_db.jddj_as_refund_info_line a 
left join  erp_db.jddj_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;

drop table if exists zhuanji_jddj_to_local ;
create table zhuanji_jddj_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_jddj a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=5) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_jddj_to_qudao ;
create table zhuanji_jddj_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_jddj a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=5) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  京东  京东到家 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 京东  京东到家 售后单处理 失败   ">> ${errologs}   
    
fi







mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- 速卖通ERP售后单

drop table if exists result_sale_order_after_sales_smt;
create table result_sale_order_after_sales_smt as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount, -- 总退款金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
case 
    when b.after_sale_type=0 then '退货退款' 
    when b.after_sale_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.zero_response = 1 then '未发秒退' else '' end weifamiaotui 

from erp_db.smt_as_refund_info_line a 
left join  erp_db.smt_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;

drop table if exists zhuanji_smt_to_local ;
create table zhuanji_smt_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_smt a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=7) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_smt_to_qudao ;
create table zhuanji_smt_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_smt a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=7) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  速卖通 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 速卖通 售后单处理 失败   ">> ${errologs}   
    
fi








mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- 快手ERP售后单

drop table if exists result_sale_order_after_sales_ks;
create table result_sale_order_after_sales_ks as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount,  -- 总退款金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
case 
    when b.after_sale_type=0 then '退货退款' 
    when b.after_sale_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.zero_response = 1 then '未发秒退' else '' end weifamiaotui 

from erp_db.ks_as_refund_info_line a 
left join  erp_db.ks_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;

drop table if exists zhuanji_ks_to_local ;
create table zhuanji_ks_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_ks a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=8) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_ks_to_qudao ;
create table zhuanji_ks_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_ks a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=8) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  快手 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 快手 售后单处理 失败   ">> ${errologs}   
    
fi












mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- TEMUERP售后单

drop table if exists result_sale_order_after_sales_temu;
create table result_sale_order_after_sales_temu as 

select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount,  -- 总退款金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.after_sale_status_desc refund_status, -- 售后状态
case 
    when b.after_sale_type=0 then '退货退款' 
    when b.after_sale_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
case when a.refund_local=1 then '是' 
     when a.refund_local=0 then '否' 
     end refund_local,  -- 本地退标识
 case when b.zero_response = 1 then '未发秒退' else '' end weifamiaotui 

from erp_db.temu_as_refund_info_line a 
left join  erp_db.temu_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;

drop table if exists zhuanji_temu_to_local ;
create table zhuanji_temu_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_temu a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=10) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_temu_to_qudao ;
create table zhuanji_temu_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_temu a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=10) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  temu 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " temu 售后单处理 失败   ">> ${errologs}   
    
fi








mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;



-- de_wuERP售后单


drop table if exists result_sale_order_after_sales_dewu;
create table result_sale_order_after_sales_dewu as 


select 
a.id as_refund_line_id,
FROM_UNIXTIME(floor(b.created_time/1000)) date_time,
b.total_amount,  -- 总退款金额 
b.sale_channel_id,
b.ro_name,-- 售后单号
b.refund_id,-- 退款编号
b.refund_status, -- 售后状态
case 
    when b.refund_type=20 or b.refund_type=2 then '退货退款' 
    when b.refund_type=10 or b.refund_type=1 then '仅退款'  end good_status, -- 退款类型
a.express_number refund_express_number,-- 退货物流
c.pay_amount, -- 销售价格
b.created_time, -- 售后申请时间
c.name, -- so_id
c.express_number, -- 发货物流单号
'' refund_local,
''  weifamiaotui 

from erp_db.de_wu_as_refund_info_line a 
left join  erp_db.de_wu_as_refund_info b on a.as_id=b.id 
left join  erp_db.sale_order_new c on a.so_id=c.id 
where FROM_UNIXTIME(floor(b.created_time/1000)) >'2026-01-01 00:00:00' 
;


drop table if exists zhuanji_de_wu_to_local ;
create table zhuanji_de_wu_to_local as 

select * from (
select a.name,b.express_number_to_local from  result_sale_order_after_sales_dewu a 
left join 
(select type,as_refund_line_id,express_number_to_local from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=9) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_local is not null;


drop table if exists zhuanji_de_wu_to_qudao ;
create table zhuanji_de_wu_to_qudao as 
select * from (
select a.name,b.express_number_to_samp from  result_sale_order_after_sales_dewu a 
left join 
(select type,as_refund_line_id,express_number_to_samp from erp_db.as_refund_samp_forward where express_number_to_samp is not null and type=9) b 
on a.as_refund_line_id=b.as_refund_line_id
) 
xxx 
where xxx.express_number_to_samp is not null;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "  得物 售后单处理  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 得物 售后单处理 失败   ">> ${errologs}   
    
fi




















BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;


-- ERP售后单汇总表

truncate table result_sale_order_after_sales_total;
insert into  result_sale_order_after_sales_total 
select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,init_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_tianmao_taobao;

insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,init_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_pdd;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,init_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_doudian;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_jd;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_jddj;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_smt;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_ks;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_temu;
insert into result_sale_order_after_sales_total select as_refund_line_id,date_time,total_amount,sale_channel_id,ro_name,refund_id,refund_status,good_status,refund_express_number,pay_amount,created_time,name,express_number,refund_local,weifamiaotui from result_sale_order_after_sales_dewu;



;

update result_sale_order_after_sales_total set refund_express_number=replace(refund_express_number,'-1','') where refund_express_number like '%-1';
update result_sale_order_after_sales_total set refund_express_number=replace(refund_express_number,'-0','') where refund_express_number like '%-0';
update result_sale_order_after_sales_total set refund_express_number=''  where refund_express_number = 'NULL';
update result_sale_order_after_sales_total set refund_express_number=''  where length(refund_express_number) <10 and refund_express_number !='';






drop table if exists result_sale_order_after_sales_total01;
create table result_sale_order_after_sales_total01 as 
select 
max(date_time) date_time,
max(total_amount) max_refund_amount,
group_concat(distinct sale_channel_id) sale_channel_id,
group_concat(distinct ro_name) ro_name,
group_concat(distinct refund_id) refund_id,
group_concat(distinct refund_status) refund_status,
group_concat(distinct good_status) good_status,
group_concat(distinct refund_express_number) refund_express_number,
max(pay_amount) pay_amount,
max(init_time) init_time,
name,
group_concat(distinct express_number) express_number,
group_concat(distinct refund_local) refund_local,
group_concat(distinct weifamiaotui) weifamiaotui  
from result_sale_order_after_sales_total 
where name is not null 
group by name ;







-- 天猫  淘宝  拼多多   抖店    京东  京东到家  转寄到渠道    快递单号
truncate table zhuanji_total_to_qudao;
insert into  zhuanji_total_to_qudao 
select * from zhuanji_tianmao_taobao_to_qudao union all 
select * from zhuanji_pdd_to_qudao union all 
select * from zhuanji_doudian_to_qudao union all 
select * from zhuanji_jd_to_qudao union all 
select * from zhuanji_jddj_to_qudao union all 
select * from zhuanji_smt_to_qudao union all 
select * from zhuanji_ks_to_qudao union all 
select * from zhuanji_temu_to_qudao union all 
select * from zhuanji_de_wu_to_qudao 
;


-- 天猫  淘宝  拼多多   抖店    京东  京东到家  买家退回到本地仓快递单号    快递单号
truncate table  maijia_total_to_local;
insert into  maijia_total_to_local 
select * from zhuanji_tianmao_taobao_to_local union all 
select * from zhuanji_pdd_to_local union all 
select * from zhuanji_doudian_to_local union all 
select * from zhuanji_jd_to_local union all 
select * from zhuanji_jddj_to_local union all 
select * from zhuanji_smt_to_local union all 
select * from zhuanji_ks_to_local union all 
select * from zhuanji_temu_to_local union all 
select * from zhuanji_de_wu_to_local 

;


"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "   ERP售后单汇总表 计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " ERP售后单汇总表  计算 失败   ">> ${errologs}   
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`



mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from result_sale_order_after_sales_tianmao_taobao;" >/data/exchange/result_sale_order_after_sales_tianmao_taobao.txt
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  result_sale_order_after_sales_tianmao_taobao;load data local infile '/data/exchange/result_sale_order_after_sales_tianmao_taobao.txt' into table result_sale_order_after_sales_tianmao_taobao character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from result_sale_order_after_sales_total;" >/data/exchange/result_sale_order_after_sales_total.txt
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  result_sale_order_after_sales_total;load data local infile '/data/exchange/result_sale_order_after_sales_total.txt' into table result_sale_order_after_sales_total character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from zhuanji_total_to_qudao;" >/data/exchange/zhuanji_total_to_qudao.txt
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  zhuanji_total_to_qudao;load data local infile '/data/exchange/zhuanji_total_to_qudao.txt' into table zhuanji_total_to_qudao character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from maijia_total_to_local;" >/data/exchange/maijia_total_to_local.txt
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  maijia_total_to_local;load data local infile '/data/exchange/maijia_total_to_local.txt' into table maijia_total_to_local character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "   数据迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 数据迁移 失败   ">> ${errologs}   
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

drop table if exists result_sale_order_after_sales_total01;
create table result_sale_order_after_sales_total01 as 
select 
max(date_time) date_time,
max(total_amount) max_refund_amount,
group_concat(distinct sale_channel_id) sale_channel_id,
group_concat(distinct ro_name) ro_name,
group_concat(distinct refund_id) refund_id,
group_concat(distinct refund_status) refund_status,
group_concat(distinct good_status) good_status,
group_concat(distinct refund_express_number) refund_express_number,
max(pay_amount) pay_amount,
max(init_time) init_time,
name,
group_concat(distinct express_number) express_number,
group_concat(distinct refund_local) refund_local,
group_concat(distinct weifamiaotui) weifamiaotui  
from result_sale_order_after_sales_total 
where name is not null 
group by name ;




"




if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " ERP售后单汇总表 取最新状态  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " ERP售后单汇总表 取最新状态 失败   ">> ${errologs}   
    
fi







##########################################################################################################################
#中文描述：本地仓 基础表 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401






BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -e "
use big_data;
-- 本地仓 成本 查询

truncate table local_warehouse01 ;
insert into local_warehouse01 
select 
FROM_UNIXTIME(floor(x.last_upd_time/1000)) date_time,
 x.last_upd_time deliverTime, 
 x.mjBarcode as mjBarcode,
 a.name as saleOrderNewName, 
 d.cost_price as receiptPrice
 from erp_db.stock_deliver x 

 left join erp_db.sale_order_new  a 
        on (x.origin_id = a.id and x.type = 'SALE_ORDER') 

left join erp_db.sale_order_new b 
        on (x.sale_new_id = b.id and x.type = 'SAMP_FORWARD') 

left outer join erp_db.as_refund_samp_forward  c 
        on (x.origin_id = c.id and x.type = 'SAMP_FORWARD' and c.forward_or_not = true) 

left outer join erp_db.product  e 
        on x.product_id = e.id 

left outer join erp_db.stock_production_mlot  d 
        on x.mjBarcode = d.mj_barcode 


left outer join erp_db.sys_user  f 
        on f.id = x.last_upd_uid where (x.created_time >= 1748707200000 and x.state = 'DELIVERED' and x.warehouse_id in (0, 1, 2));
                
                
delete from local_warehouse01 where so_order_id is null;
delete from local_warehouse01 where mjBarCode like 'ty%';
delete from local_warehouse01 where mjBarCode like 'zz%';
delete from local_warehouse01 where mjBarCode like 'hh%';
delete from local_warehouse01 where mjBarCode like 'dh%';

-- 本地仓出库 记录查询  并关联  公司码 成本价  
        

"

mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;select * from local_warehouse01;" >/data/exchange/local_warehouse01.txt
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  local_warehouse01;load data local infile '/data/exchange/local_warehouse01.txt' into table local_warehouse01 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';


update local_warehouse01 a ,
(select mjBarcode,max(costprice) costprice from local_warehouse_out_in_pop.local_warehouse_mjbarcode02 group by mjBarcode) b 
set a.costprice=b.costprice where a.costprice=0 and a.mjBarcode=b.mjBarcode;

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓成本数据 etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓成本数据 etl 失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  本地仓入库基础表分析
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "

truncate table  stock_location;
insert into stock_location select * from erp_db.stock_location where  scrap_location  is null ;
insert into stock_location select * from erp_db.stock_location where  scrap_location  =0 ;

truncate table rugen_local_warehouse_enter;
insert into  rugen_local_warehouse_enter 
SELECT
   FROM_UNIXTIME(floor(a.created_time/1000)) date_time,
    b.name AS pickingName,
    e.default_code AS productCode,
    e.brand AS brand,
    e.category AS category,
    a.mj_barcode AS mjBarcode,
    a.picking_type_id AS pickingTypeId,
    j.name AS pickingTypeName,
    a.stored,
    a.bound_location_id AS boundLocationId,
    d.location_id AS nowLocationId,
    h.barcode AS barCode,
    h.location_type AS locationType,
    b.location_id AS sourceLocationId,
    a.bound_location_time AS boundLocationTime,
    a.bound_location_uid AS boundLocationUid,
    a.created_time AS createTime,
    g.true_name AS boundLocationUserName,
    b.created_time AS pickingCreatedTime,
    b.state AS pickingState,
    b.origin AS pickingOrigin,
    b.tid AS pickingTid,
    b.picking_type_id AS pickTypeId,
    replace(b.express_number,'-1','') AS pickingExpressNumber,
    f.id AS saleId,
    f.name AS saleName,
    f.origin AS origin,
    f.picking_source AS pickingSource,
    f.ps_gx_warehouse_channel AS psGxWarehouseChannel,
    f.sale_channel_id AS saleChannelId,
    f.created_time AS createdTime,
    k.channel_name AS saleChannelName,
    d.cost_price AS costPrice,
    d.remark AS flawedRemark,
    e.tag_price AS tagPrice,
    g.true_name AS createdUName,
    c.id AS coId,
    c.platform AS coPlatform,
    c.warehouse_id AS coWarehouse,
    h.scrap_location AS scrapLocation 
FROM
    erp_db.stock_bcw_incoming_pending a
    LEFT join erp_db.stock_picking b ON a.picking_id = b.id
    LEFT join erp_db.samp_cut_goods_order c ON b.origin = c.co_name
    LEFT join erp_db.stock_production_mlot d ON a.mj_barcode = d.mj_barcode
    LEFT join erp_db.product e ON a.product_id = e.id
    LEFT join erp_db.sale_order_new f ON b.origin = f.as_refund_no
    LEFT join erp_db.sys_user g ON a.created_uid = g.id
    LEFT join stock_location h ON d.location_id = h.id
    LEFT join erp_db.stock_picking_type j ON a.picking_type_id = j.id
    LEFT join erp_db.sale_channel k ON k.id = f.sale_channel_id 
WHERE
    a.created_time >= 1767196800000  AND a.id IS NOT NULL 
   --  and d.remark is null  产品实例表  备注字段不参与判断 

-- GROUP BY a.mj_barcode 
ORDER BY
    a.id DESC;
 



update rugen_local_warehouse_enter set salename = '' where saleName='SO006291947';
update rugen_local_warehouse_enter set origin = '' where origin='2249750463680715992';
update rugen_local_warehouse_enter set origin = '' where origin is null;
update rugen_local_warehouse_enter set salename = '' where salename is null;



use big_data;
update rugen_local_warehouse_enter set saleName = replace(SUBSTRING_INDEX(pickingOrigin,'-',1),'ty','') where pickingOrigin like '%SO%' and saleName = '';



with test as (
select x.ro_name,x.sn from (select ro_name,group_concat(distinct name) sn from result_sale_order_after_sales_total group by ro_name) x where x.sn not like '%,%'
)

update 
rugen_local_warehouse_enter a,test b 

set a.salename=b.sn 

where 
a.salename = '' and 
a.pickingOrigin regexp 'RO' and 
a.pickingOrigin =b.ro_name
;


-- 通过退货快递单号 匹配 售后单 退货单号 匹配出  so单号

with test as (select refund_express_number,name from result_sale_order_after_sales_total where refund_express_number is not null)
update 
rugen_local_warehouse_enter a ,
test b 

set 
a.salename=b.name 

where  a.pickingexpressnumber=b.refund_express_number and a.salename = '';


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 入库 基础表  计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 入库 基础表  计算 失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  本地仓质量问题 货品入库 分析
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
drop table if exists rugen_local_warehouse_quality_problem;
create table rugen_local_warehouse_quality_problem as 
SELECT 
    FROM_UNIXTIME(floor(a.created_time/1000)) date_time,
    b.name AS pickingName,
    f.default_code AS productCode,
    f.brand AS brand,
    f.category AS category,
    a.mj_barcode AS mjBarcode,
    a.picking_type_id AS pickingTypeId,
    j.name AS pickingTypeName,
    a.stored,
    a.bound_location_id AS boundLocationId,
    d.location_id AS nowLocationId,
    h.barcode AS barCode,
    h.location_type AS locationType,
    b.location_id AS sourceLocationId,
    a.bound_location_time AS boundLocationTime,
    a.bound_location_uid AS boundLocationUid,
    a.created_time AS createTime,
    g.true_name AS boundLocationUserName,
    b.created_time AS pickingCreatedTime,
    b.state AS pickingState,
    b.origin AS pickingOrigin,
    b.tid AS pickingTid,
    b.picking_type_id AS pickTypeId,
    b.express_number AS pickingExpressNumber,
    e.id AS saleId,
    e.name AS saleName,
    e.origin AS origin,
    e.picking_source AS pickingSource,
    e.ps_gx_warehouse_channel AS psGxWarehouseChannel,
    e.sale_channel_id AS saleChannelId,
    e.created_time AS createdTime,
    k.channel_name AS saleChannelName,
    d.cost_price AS costPrice,
    d.remark AS flawedRemark,
    f.tag_price AS tagPrice,
    g.true_name AS createdUName,
    c.id AS coId,
    c.platform AS coPlatform,
    c.warehouse_id AS coWarehouse,
    h.scrap_location AS scrapLocation 
FROM
    erp_db.stock_bcw_incoming_pending a 
    LEFT JOIN erp_db.stock_picking  b ON a.picking_id = b.id
    LEFT JOIN erp_db.samp_cut_goods_order c ON b.origin = c.co_name
    LEFT JOIN erp_db.stock_production_mlot d ON a.mj_barcode = d.mj_barcode
    LEFT JOIN erp_db.product f ON a.product_id = f.id
    LEFT JOIN erp_db.sale_order_new e ON b.origin = e.as_refund_no
    LEFT JOIN erp_db.sys_user g ON a.created_uid = g.id
    LEFT JOIN erp_db.stock_location  h ON d.location_id = h.id
    LEFT JOIN erp_db.stock_picking_type j ON a.picking_type_id = j.id
    LEFT JOIN erp_db.sale_channel  k ON k.id = e.sale_channel_id 
WHERE
    (a.created_time>= 1767196800000 AND a.id IS NOT NULL ) 
    AND h.scrap_location = 1 
 
-- GROUP BY a.mj_barcode 
ORDER BY
    a.id DESC;



update rugen_local_warehouse_quality_problem set salename = '' where saleName='SO006291947';
update rugen_local_warehouse_quality_problem set origin = '' where origin='2249750463680715992';
update rugen_local_warehouse_quality_problem set origin = '' where origin is null;
update rugen_local_warehouse_quality_problem set salename = '' where salename is null;



update
rugen_local_warehouse_quality_problem a,
(select x.* from (select ro_name,group_concat(distinct name) sn from result_sale_order_after_sales_total group by ro_name) x where x.sn not like '%,%') b

set a.salename=b.sn

where
a.salename is null and
a.pickingOrigin like '%RO%' and
a.pickingOrigin =b.ro_name
;

update rugen_local_warehouse_quality_problem set saleName = replace(SUBSTRING_INDEX(pickingOrigin,'-',1),'ty','') where pickingOrigin like '%SO%' and saleName is null;


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓质量问题 入库 基础表  计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 质量问题入库 基础表  计算 失败   ">> ${errologs}   
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
## 本地仓质量问题入库表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 平台入库
select date_time,salename,mjBarcode,costprice from rugen_local_warehouse_quality_problem where salename like 'so%' ;
">/data/exchange/rugen_local_warehouse_quality_problem.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_local_warehouse_quality_problem;load data local infile '/data/exchange/rugen_local_warehouse_quality_problem.txt' into table rugen_local_warehouse_quality_problem character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 质量问题货品入库数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 质量问题货品入库数据  etl   失败   ">> ${errologs}   
    
fi





## 开发时间  2024-08-01      原因：在计算 本地仓入库货品数据时候 正面逻辑回遗漏数据
## 逆向开发      从快递单号  匹配回来的货品
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
 
truncate table  快递单号匹配so单号 ;
insert into 快递单号匹配so单号 
select express_number,so_id,as_id from erp_db.as_refund_info_line where express_number is not null and created_time > 1767196800000;


truncate table 快递单号匹配so单号_middle;
insert into 快递单号匹配so单号_middle 
select id,name,origin,order_date_time from erp_db.sale_order_new  
where order_date_time >= 1767196800000;


truncate table 快递单号匹配so单号_01;
insert into 快递单号匹配so单号_01 
SELECT
a.express_number,
c.name,
c.origin,
FROM_UNIXTIME(floor(c.order_date_time/1000)) date_time, 
f.scan_barcode
FROM 快递单号匹配so单号 a
LEFT JOIN 快递单号匹配so单号_middle c ON c.id = a.so_id 
left join erp_db.STOCK_WAIT_RECEIVE e on e.express_number=a.express_number
left join erp_db.STOCK_WAIT_RECEIVE_LINE f on f.receive_id = e.id;


delete from 快递单号匹配so单号_01 where name is null;
delete from 快递单号匹配so单号_01 where scan_barcode is null;

update 快递单号匹配so单号_01 set scan_barcode = replace(replace(scan_barcode,'{\"stockReceiveBarcodeDetailList\":[{\"mjBarcode\":\"',''),'\",\"flow\":false}]}','') ;
update 快递单号匹配so单号_01 set scan_barcode = replace(scan_barcode,'\",\"flow\"\:true}]}','');


truncate table 快递单号匹配so单号_02;
insert into 快递单号匹配so单号_02 (date_time,name,scan_barcode) 
select date_time,name,scan_barcode from 快递单号匹配so单号_01;

truncate table 快递单号匹配so单号_02_middle;
insert into 快递单号匹配so单号_02_middle 
select salename,mjBarcode,costprice from rugen_local_warehouse_enter where salename like 'so%' union 
select salename,mjBarcode,costprice from rugen_local_warehouse_quality_problem where salename like 'so%';



drop table if exists 快递单号匹配so单号_03;
create table 快递单号匹配so单号_03 as 
select date_time,name,scan_barcode,costprice from 快递单号匹配so单号_02 
where name not in (select salename from 快递单号匹配so单号_02_middle) ;

update 快递单号匹配so单号_03 a,(select mjbarcode,costprice from 快递单号匹配so单号_02_middle) b 
set a.costprice=b.costprice where a.scan_barcode=b.mjbarcode;
update 快递单号匹配so单号_03 set costprice = 0 where costprice is null;

"


## 通过快递单号逆向补充  so单号 公司码     加入到 下面  rugen_local_warehouse_ruku 表中





## 本地仓入库表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 平台入库
select date_time,salename,mjBarcode,origin,costprice from rugen_local_warehouse_enter where salename like 'so%' union 
select date_time,name,scan_barcode,'',costprice from 快递单号匹配so单号_03; 

">/data/exchange/rugen_local_warehouse_ruku.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_local_warehouse_ruku;load data local infile '/data/exchange/rugen_local_warehouse_ruku.txt' into table rugen_local_warehouse_ruku character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 入库数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 入库数据  etl   失败   ">> ${errologs}   
    
fi





####################
###################  本仓出库明细表
##################

##  本地仓出库基础表分析
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
truncate table  rugen_local_warehouse_out;
insert into  rugen_local_warehouse_out 

SELECT
    j.NAME AS pickingTypeName,
    FROM_UNIXTIME(floor( a.last_upd_time / 1000 )) date_time,

    a.mjBarcode AS mjBarcode,
    a.last_upd_time AS deliverTime,
    a.express_number AS expressNumber,
    c.origin AS origin,
    d.origin AS zjOrigin,
    c.NAME AS saleOrderNewName,
    d.NAME AS zjSaleOrderNewName,
    c.pay_amount AS payAmount,
    c.amount_total AS amountTotal,
    c.picking_source AS pickingSource,
    d.picking_source AS zjPickingSource,
    g.channel_name AS saleChannelName,
    f.cost_price AS receiptPrice 
FROM
    erp_db.stock_deliver a 
    LEFT JOIN erp_db.stock_picking b ON a.stock_picking_id = b.id
    LEFT JOIN erp_db.stock_picking_type j ON b.picking_type_id = j.id
    LEFT JOIN erp_db.sale_order_new c ON ( a.origin_id = c.id AND a.type = 'SALE_ORDER' )
    LEFT JOIN erp_db.sale_order_new d ON ( a.sale_new_id = d.id AND a.type = 'SAMP_FORWARD' )
    LEFT JOIN erp_db.as_refund_samp_forward e ON ( a.origin_id = e.id AND a.type = 'SAMP_FORWARD' AND e.forward_or_not = TRUE )
    LEFT JOIN erp_db.stock_production_mlot  f ON a.mjBarcode = f.mj_barcode
    LEFT JOIN erp_db.sale_channel  g ON c.sale_channel_id = g.id
WHERE
    (
      a.last_upd_time >= 1767196800000 
   AND a.state = 'DELIVERED' 
   AND a.warehouse_id IN ( 0, 1, 2 )) 
ORDER BY
    a.created_time DESC ;







update  rugen_local_warehouse_out  set zjSaleOrderNewName = saleOrderNewName where zjSaleOrderNewName is null;
update  rugen_local_warehouse_out  set saleOrderNewName = zjSaleOrderNewName where saleOrderNewName is null;


update  rugen_local_warehouse_out  set zjSaleOrderNewName = '' where zjSaleOrderNewName is null;
update  rugen_local_warehouse_out  set saleOrderNewName = '' where saleOrderNewName is null;






update  rugen_local_warehouse_out a,zhuanji_total_to_qudao b 


set a.zjSaleOrderNewName = b.name , a.saleOrderNewName = b.name 

where a.expressNumber = b.express_number_to_samp and a.zjSaleOrderNewName = '' and pickingtypename = '出库-即墨总仓-本地仓转寄渠道';




     "

     if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓  出库库 基础表  计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 出库 基础表  计算 失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
## 本地仓出库表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 

select * from rugen_local_warehouse_out ;
">/data/exchange/rugen_local_warehouse_out.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_local_warehouse_out;load data local infile '/data/exchange/rugen_local_warehouse_out.txt' into table rugen_local_warehouse_out character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 出库数据  etl  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 出库数据  etl   失败   ">> ${errologs}   
    
fi









## 本地仓转寄表
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 平台转寄
select date_time,zjSaleOrderNewName,mjBarcode,receiptPrice,expressNumber from rugen_local_warehouse_out 
where zjSaleOrderNewName like 'so%' and 
pickingTypeName = '出库-即墨总仓-本地仓转寄渠道';
">/data/exchange/rugen_local_warehouse_zhuanji.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_local_warehouse_zhuanji;load data local infile '/data/exchange/rugen_local_warehouse_zhuanji.txt' into table rugen_local_warehouse_zhuanji character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 转寄数据  etl  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 转寄数据  etl   失败   ">> ${errologs}   
    
fi






##   耗时较长  待优化
##  本地仓公司码 最早入库时间  计算 分析
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
truncate table  local_warehouse_mjbarcode;
insert into  local_warehouse_mjbarcode 
SELECT
    FROM_UNIXTIME(floor(a.change_time/1000)) date_time,
    a.id AS id,
    if(h.title is not null,h.title,g.name) name,
    a.mj_barcode AS mjBarcode,
    a.change_time AS changeTime,
    a.STOCK_PRODUCTION_MLOT_ID AS stockProductionMlotId,
    replace(f.express_number,'-1','') AS pickingExpressNumber    

FROM
    erp_db.stock_production_mlot_location_change_log a 
     left join erp_db.stock_inventory_new h on a.inventory_Id=h.id 
           left join erp_db.stock_picking f on f.id=a.picking_id 
   left join erp_db.stock_picking_type g on g.id=f.picking_type_id 
-- where a.change_time > 1709222400000  
-- WHERE a.STOCK_PRODUCTION_MLOT_ID IN (550283 )     
-- order by a.change_time
;


"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓公司码 最早入库时间  计算  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓公司码 最早入库时间  计算 失败   ">> ${errologs}   
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
select date_time,id,name,mjbarcode,changetime,stockProductionMlotId,pickingExpressNumber from  local_warehouse_mjbarcode;
">/data/exchange/local_warehouse_mjbarcode.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  local_warehouse_mjbarcode;load data local infile '/data/exchange/local_warehouse_mjbarcode.txt' into table local_warehouse_mjbarcode character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


### 开窗获取公司码 最早入库时间

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;
truncate table local_warehouse_mjbarcode01;
insert into local_warehouse_mjbarcode01 
select min(x.sn) date_time,x.mjBarcode from 
(select *,min(date_time) over (partition by stockProductionMlotId ) sn from local_warehouse_mjbarcode) x 
group by x.mjBarcode;
"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓公司码 最早入库时间  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓公司码 最早入库时间   etl   失败   ">> ${errologs}   
    
fi




## 公司码最晚入库时间

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;
truncate table local_warehouse_mjbarcode_lastest_entertime;
insert into local_warehouse_mjbarcode_lastest_entertime 


with test as (
select 
*,
max(date_time) over (partition by stockProductionMlotId ) sn,
row_number() over (partition by stockProductionMlotId order by date_time desc) sn_date 
from local_warehouse_mjbarcode 
where name like '%入库%' 
-- and  stockProductionMlotId='480899'
)

select x.date_time,x.mjbarcode,y.pickingExpressNumber 

from (select max(sn) date_time,mjBarcode,group_concat(distinct stockProductionMlotId) stockProductionMlotId from  test group by mjBarcode) x 
left join  
(select stockProductionMlotId,pickingExpressNumber from test where sn_date=1) y 
on x.stockProductionMlotId=y.stockProductionMlotId


"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓公司码 最晚入库时间  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓公司码 最晚入库时间   etl   失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
## 本地供销仓转寄退回表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 平台转寄
select date_time,salename,mjBarcode,costprice,pickingExpressNumber from rugen_local_warehouse_enter 
where salename like 'so%' and 
pickingTypeName = '入库-即墨总仓-供销转寄退回';
">/data/exchange/rugen_local_warehouse_zhuanji_return.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_local_warehouse_zhuanji_return;load data local infile '/data/exchange/rugen_local_warehouse_zhuanji_return.txt' into table rugen_local_warehouse_zhuanji_return character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 本地仓 转寄退回数据  etl  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 本地仓 转寄退回数据  etl   失败   ">> ${errologs}   
    
fi





##########################################################################################################################
#中文描述：3150赔付 基础表 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401



## -- 3150 赔付数据获取
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -N -e "
use big_data;
drop table if exists result_sale_order_3150;
create table result_sale_order_3150 as 
select 
min(FROM_UNIXTIME(floor(created_time/1000))) date_time,
so_name,
group_concat(concat(so_name,'--',payment_type,':',amount)) payment_type,
sum(-ABS(amount)) amount 
 from erp_db.AS_REFUND_CASH_RETURN  where state = '已打款' group by so_name ;

select * from big_data.result_sale_order_3150;
 " >/data/exchange/result_sale_order_3150.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  result_sale_order_3150;load data local infile '/data/exchange/result_sale_order_3150.txt' into table result_sale_order_3150 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 3150补款数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 3150补款数据 etl 失败   ">> ${errologs}   
    
fi



##########################################################################################################################
#中文描述：线下补款 基础表 实时获取
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401
 
 ## -- 补款  财务确认
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
 mysql -hRDS_HOST -ubig_data -pPASS -N -e "
 select 
 FROM_UNIXTIME(floor(created_time/1000)) date_time,
 substring(so_name,1,11) so_name,
 supplementary_amount
 from erp_db.SUPPLEMENT_NOTE 
  where examine=1 or examine is null;

 " >/data/exchange/result_sale_order_supplement.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  result_sale_order_supplement;load data local infile '/data/exchange/result_sale_order_supplement.txt' into table result_sale_order_supplement character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 线下补款数据  etl  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 线下补款数据 etl 失败   ">> ${errologs}   
    
fi





##########################################################################################################################
#中文描述：菜鸟仓数据 处理
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401
#############################################################################################################################



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  菜鸟仓入库 分析
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
truncate table rugen_cainiao_enter;
insert into rugen_cainiao_enter 
select 
FROM_UNIXTIME(floor(created_time/1000)) date_time,
origin_order_no so_order_id,
express_no 

from erp_db.cainiao_return_order 

where status = 'FULFILLED'  and FROM_UNIXTIME(floor(created_time/1000)) > '2026-01-01 00:00:00'


order by created_time desc;
   
"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 菜鸟仓 入库 基础表  计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 菜鸟仓入库 基础表  计算 失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
## 菜鸟仓入库表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 菜鸟仓入库
select * from rugen_cainiao_enter where so_order_id like 'so%' ;
">/data/exchange/rugen_cainiao_enter.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_cainiao_enter;load data local infile '/data/exchange/rugen_cainiao_enter.txt' into table rugen_cainiao_enter character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 菜鸟货品入库数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 菜鸟货品入库数据  etl   失败   ">> ${errologs}   
    
fi






BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  菜鸟仓入库转寄 分析
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
drop table if exists rugen_cainiao_zhuanji;
create table rugen_cainiao_zhuanji as 
select a.pre_delivery_order_code ro_num,b.name so_order_id 

from 
(select pre_delivery_order_code,FROM_UNIXTIME(floor(created_time/1000)) date_time 
from erp_db.cainiao_deliver_order 
where 
pre_delivery_order_code like 'RO%' and 
FROM_UNIXTIME(floor(created_time/1000)) > '2026-01-01 00:00:00'  AND 
status = 'DELIVERED') a 

left join 

(select ro_name,name from result_sale_order_after_sales_tianmao_taobao union 
select ro_name,name from result_sale_order_after_sales_pdd union 
select ro_name,name from result_sale_order_after_sales_doudian union 
select ro_name,name from result_sale_order_after_sales_jd union 
select ro_name,name from result_sale_order_after_sales_jddj) b 

on a.pre_delivery_order_code=b.ro_name;


delete from rugen_cainiao_zhuanji where so_order_id is null;




-- delete语句 删除 转寄又退回的订单
delete from rugen_cainiao_zhuanji where so_order_id in 

-- 这个sql 获取菜鸟转寄又退回的so单号 
(select origin_order_no from (
select origin_order_no,group_concat(distinct return_order_id)  ,count(distinct return_order_id) 
from erp_db.cainiao_return_order 
where origin_order_no !='' and  status = 'FULFILLED'  and FROM_UNIXTIME(floor(created_time/1000)) > '2026-01-01 00:00:00'
group by origin_order_no 
having count(distinct return_order_id) >1) x )



"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 菜鸟仓 转寄 基础表  计算  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 菜鸟仓转寄 基础表  计算 失败   ">> ${errologs}   
    
fi

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
## 菜鸟仓转寄表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 菜鸟仓入库
select * from rugen_cainiao_zhuanji ;
">/data/exchange/rugen_cainiao_zhuanji.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_cainiao_zhuanji;load data local infile '/data/exchange/rugen_cainiao_zhuanji.txt' into table rugen_cainiao_zhuanji character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 菜鸟货品入库数据  etl  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 菜鸟货品入库数据  etl   失败   ">> ${errologs}   
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
##  物流详细信息表
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e " 
-- 物流详细信息表
select 
standard_express,
group_concat(distinct courier_state) courier_state 
from erp_db.express_monitor where courier_state is not null and courier_state != 'NULL' and created_time > 1767196800000  group by standard_express ;">/data/exchange/rugen_express_monitor.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  rugen_express_monitor;load data local infile '/data/exchange/rugen_express_monitor.txt' into table rugen_express_monitor character set utf8mb4 fields terminated by '\t' lines terminated by '\n';
update rugen_express_monitor set courier_state='暂无' where courier_state = '暂无轨迹信息';
"





if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 物流详细信息表  etl  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 物流详细信息表  etl   失败   ">> ${errologs}   
    
fi






## 在此处增加 不明来源入库匹配
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "

drop table if exists 不明来源入库匹配;
create table 不明来源入库匹配 as 
SELECT
    b.express_number AS expressNumber,
    b.express_company AS expressName,
    a.sale_channel_id AS saleChannelId,
    a.product_id AS productId,
    a.default_code AS defaultCode,
    a.mj_barcode AS mjBarcode,
    FROM_UNIXTIME(floor(a.created_time/1000)) AS createTime,
    a.remark AS remark,
        SUBSTRING(concat('SO',substring_index(replace(remark,'so','SO'),'SO',-1)),1,11) so_order_id 
FROM
    erp_db.as_unknow_source_pretreatment_line a
    LEFT  JOIN erp_db.as_unknow_source_pretreatment b ON a.as_unknown_id = b.id
        
        where remark like '%so%' 
        and FROM_UNIXTIME(floor(a.created_time/1000)) > '2026-01-01 00:00:00'
        and remark not like '%山东仓%';



select so_order_id,group_concat(distinct mjbarcode) mjbarcode,max(createtime) createtime from 不明来源入库匹配 group by so_order_id; ">/data/exchange/不明来源入库匹配.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  不明来源入库匹配;load data local infile '/data/exchange/不明来源入库匹配.txt' into table 不明来源入库匹配 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 不明来源数据计算 迁移  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 不明来源数据计算 迁移   失败   ">> ${errologs}   
    
fi







## ERP调货单   天猫共鞋 已打款  数据更新  
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

#mysql -hRDS_HOST -ubig_data -pPASS -N -e "
#use big_data;select so_name,'222222' orderid,transfer_goods_price,'ERP' type from erp_db.so_transfer_goods_order where artificial_status ='已打款'  and created_time >= 1767196800000 and  transfer_goods_price is not null and transfer_goods_source !='滔搏' and transfer_goods_source !='滔博' and transfer_goods_source !='共鞋马学林' and substring(FROM_UNIXTIME(floor(created_time/1000)),1,10) != substring(NOW(),1,10) ;" >/data/exchange/天猫共鞋_已打款数据.txt

#mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  天猫共鞋_已打款数据;load data local infile '/data/exchange/天猫共鞋_已打款数据.txt' into table 天猫共鞋_已打款数据 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


#mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "use rugen; delete from 共鞋调货 where type = 'ERP'; insert into 共鞋调货 select * from  天猫共鞋_已打款数据;drop table if exists 共鞋重复so;create table 共鞋重复so as select * from (select *,ROW_NUMBER() over (partition by so_order_id order by transactionamount desc) sn  from 共鞋调货) x  where x.sn =1 ;truncate table 共鞋调货;insert into 共鞋调货 select so_order_id,orderid,transactionamount,type from 共鞋重复so;"



mysql -hRDS_HOST -ubig_data -pPASS -N -e "use big_data;

drop table if exists 共鞋调货支付宝数据89_95;
create table 共鞋调货支付宝数据89_95 as 
select substring(remark,1,11) so_order_id,business_serial_number,expenditure_amount  from erp_db.tong_ye_ali_pay_statement_of_account_info where (tong_ye_ali_pay_info_id = 89 or tong_ye_ali_pay_info_id = 95)  and occurrence_timestr >= '2026-01-01 00:00:00' and remark not like '%代发%';


update  共鞋调货支付宝数据89_95 set so_order_id ='SO007710763' where business_serial_number = '20250409020070011520750077513545';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007710753' where business_serial_number = '20250409020070011520060049576150';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007710482' where business_serial_number = '20250409020070011520530086125273';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007709639' where business_serial_number = '20250409020070011520900083184731';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007705770' where business_serial_number = '20250409020070011520620053274037';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007705435' where business_serial_number = '20250409020070011520380044630074';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704896' where business_serial_number = '20250409020070011520720021303959';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704826' where business_serial_number = '20250409020070011520720020938615';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704749' where business_serial_number = '20250409020070011520620053416418';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704564' where business_serial_number = '20250409020070011520530086135140';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704438' where business_serial_number = '20250409020070011520620053220624';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704274' where business_serial_number = '20250409020070011520340031447428';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007704177' where business_serial_number = '20250409020070011520070096246901';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007703717' where business_serial_number = '20250409020070011520720021087619';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007702210' where business_serial_number = '20250409020070011520490027108457';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007701678' where business_serial_number = '20250409020070011520320059986267';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007701550' where business_serial_number = '20250409020070011520000063916684';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007701379' where business_serial_number = '20250409020070011520490027407765';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007699111' where business_serial_number = '20250409020070011520410003685747';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007695402' where business_serial_number = '20250409020070011520860060492816';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007693625' where business_serial_number = '20250409020070011520780064974436';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007693048' where business_serial_number = '20250409020070011520910026297019';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007710657' where business_serial_number = '20250409020070011520050015558480';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007702510' where business_serial_number = '20250409020070011520530086029005';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007705727' where business_serial_number = '20250409020070011520470016046999';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007812317' where business_serial_number = '20250422020070011520950084045032';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007809200' where business_serial_number = '20250422020070011520130048777523';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007809114' where business_serial_number = '20250422020070011520330067279650';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808974' where business_serial_number = '20250422020070011520950083845254';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808939' where business_serial_number = '20250422020070011520870009960349';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808642' where business_serial_number = '20250422020070011520060020631018';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808330' where business_serial_number = '20250422020070011520660057277592';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808126' where business_serial_number = '20250422020070011520130049028654';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007808142' where business_serial_number = '20250422020070011520660057602483';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007805894' where business_serial_number = '20250422020070011520280083483474';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007805889' where business_serial_number = '20250422020070011520370066519159';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007805620' where business_serial_number = '20250422020070011520950084080105';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007805475' where business_serial_number = '20250422020070011520180053413655';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007804857' where business_serial_number = '20250422020070011520470005865779';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007804848' where business_serial_number = '20250422020070011520490003813763';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007804849' where business_serial_number = '20250422020070011520920067034225';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007804086' where business_serial_number = '20250422020070011520200076734420';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007803880' where business_serial_number = '20250422020070011520430057540164';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007803668' where business_serial_number = '20250422020070011520040031469898';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007800292' where business_serial_number = '20250422020070011520580024005058';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007800130' where business_serial_number = '20250422020070011520810001584238';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007799776' where business_serial_number = '20250422020070011520880052819194';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007799716' where business_serial_number = '20250422020070011520290002717538';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007799387' where business_serial_number = '20250422020070011520990080552231';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007797876' where business_serial_number = '20250422020070011520470006091805';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007796660' where business_serial_number = '20250422020070011520490003939730';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007796627' where business_serial_number = '20250422020070011520040031459384';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007796509' where business_serial_number = '20250422020070011520660057396430';
update  共鞋调货支付宝数据89_95 set so_order_id ='SO007708924' where business_serial_number = '20250409020070011520830034858220';



-- 公司店铺调货表  刀锋战士 打款支付宝错误  需要人工 调整
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007964695' where business_serial_number = '20250514020070011580290045910116';    -- SO007969854
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007969262' where business_serial_number = '20250514020070011580490043992319';    -- SO007964695
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007967861' where business_serial_number = '20250514020070011580660089104888';    -- SO007969262
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007968708' where business_serial_number = '20250514020070011580950028270495';    -- SO007967861
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007967254' where business_serial_number = '20250514020070011580210017023831';    -- SO007968708
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007967017' where business_serial_number = '20250514020070011580680013580445';    -- SO007967254
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007966771' where business_serial_number = '20250514020070011580240044323946';    -- SO007967017
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007966648' where business_serial_number = '20250514020070011580380054600815';    -- SO007966771
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007966111' where business_serial_number = '20250514020070011580930065628943';    -- SO007966648
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007964683' where business_serial_number = '20250514020070011580940032908681';    -- SO007966111
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007977031' where business_serial_number = '20250514020070011580860003823076';    -- SO007964683
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007976349' where business_serial_number = '20250514020070011580420048792805';    -- SO007977031
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007974185' where business_serial_number = '20250514020070011580060046749735';    -- SO007976349
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007972170' where business_serial_number = '20250514020070011580040005610898';    -- SO007974185
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971970' where business_serial_number = '20250514020070011580230077107439';    -- SO007972170
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971636' where business_serial_number = '20250514020070011580240044460921';    -- SO007971970
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971560' where business_serial_number = '20250514020070011580110083472052';    -- SO007971636
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007965940' where business_serial_number = '20250514020070011580680013444974';    -- SO007971560
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971038' where business_serial_number = '20250514020070011580890012281897';    -- SO007965940
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971039' where business_serial_number = '20250514020070011580890012009178';    -- SO007971038
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007971003' where business_serial_number = '20250514020070011580020009809677';    -- SO007971039
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007970788' where business_serial_number = '20250514020070011580430023368298';    -- SO007971003
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007970587' where business_serial_number = '20250514020070011580860003958729';    -- SO007970788
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007960083' where business_serial_number = '20250514020070011580540060829847';    -- SO007970587
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007959953' where business_serial_number = '20250514020070011580700004328864';    -- SO007960083
update 共鞋调货支付宝数据89_95 set so_order_id = 'SO007959330' where business_serial_number = '20250514020070011580220089975956';    -- SO007959953




"

mysql -hRDS_HOST -ubig_data -pPASS -N -e " use big_data;
select so_order_id,GROUP_CONCAT(distinct business_serial_number) orderid,SUM(expenditure_amount) transactionamount,'共鞋调货' type from 共鞋调货支付宝数据89_95 group by so_order_id;" >/data/exchange/天猫共鞋_已打款数据.txt



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  共鞋调货;load data local infile '/data/exchange/天猫共鞋_已打款数据.txt' into table 共鞋调货 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

-- insert into 共鞋调货 select * from 共鞋调货订单通过公司运动钱润远抵扣250519_250521;

update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007876023';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007875920';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007875752';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007875254';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007875012';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007874876';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007874875';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007874874';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007874522';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007873450';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007879555';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007879477';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007879334';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007879109';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007878850';
update 共鞋调货  set  transactionamount = transactionamount + 470 where so_order_id = 'SO007881803';
update 共鞋调货  set  transactionamount = transactionamount + 470 where so_order_id = 'SO007881791';
update 共鞋调货  set  transactionamount = transactionamount + 358 where so_order_id = 'SO007881755';
update 共鞋调货  set  transactionamount = transactionamount + 470 where so_order_id = 'SO007881737';
update 共鞋调货  set  transactionamount = transactionamount + 465 where so_order_id = 'SO007882815';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880978';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007880977';
update 共鞋调货  set  transactionamount = transactionamount + 410 where so_order_id = 'SO007884294';
update 共鞋调货  set  transactionamount = transactionamount + 470 where so_order_id = 'SO007898266';
update 共鞋调货  set  transactionamount = transactionamount + 75 where so_order_id = 'SO007902635';
update 共鞋调货  set  transactionamount = transactionamount + 406 where so_order_id = 'SO007905642';
update 共鞋调货  set  transactionamount = transactionamount + 517 where so_order_id = 'SO007870827';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007803198';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007796900';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007821720';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007821722';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007821723';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007821725';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007826997';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007864133';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007864134';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007863105';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007862126';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007862038';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007863889';
update 共鞋调货  set  transactionamount = transactionamount + 180 where so_order_id = 'SO007826490';
update 共鞋调货  set  transactionamount = transactionamount + 390 where so_order_id = 'SO007865155';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007870080';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007863103';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007870420';
update 共鞋调货  set  transactionamount = transactionamount + 390 where so_order_id = 'SO007867381';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007864131';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007879520';
update 共鞋调货  set  transactionamount = transactionamount + 465 where so_order_id = 'SO007878662';
update 共鞋调货  set  transactionamount = transactionamount + 350 where so_order_id = 'SO007879621';
update 共鞋调货  set  transactionamount = transactionamount + 470 where so_order_id = 'SO007882249';
update 共鞋调货  set  transactionamount = transactionamount + 358 where so_order_id = 'SO007881592';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880010';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880007';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880005';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880004';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007880000';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007879978';
update 共鞋调货  set  transactionamount = transactionamount + 390 where so_order_id = 'SO007827516';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007864845';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007863893';
update 共鞋调货  set  transactionamount = transactionamount + 420 where so_order_id = 'SO007863879';
update 共鞋调货  set  transactionamount = transactionamount + 408 where so_order_id = 'SO007895505';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007895507';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007895509';
update 共鞋调货  set  transactionamount = transactionamount + 393 where so_order_id = 'SO007895506';
update 共鞋调货  set  transactionamount = transactionamount + 390 where so_order_id = 'SO007827737';







"





if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 共鞋调货数据计算  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 共鞋调货数据计算   失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST_2 -uroot -pPASS -D bigdata_mt -e "
-- 杰之行订单表 externalOrderNo 抖店订单 部分不是SO  是source  需要特殊处理
select x.orderid,x.transactionAmount from 
(select orderid,sum(transactionamount) transactionamount from dugdb.jiezhixing_account where time >= '2026-01-01 00:00:00' group  by orderid) x 
inner join (select * from dugdb.jiezhixing_samp_order where externalOrderNo like '%抖店%' and externalOrderNo not like 'SO%') y 
on x.orderid=y.orderno; " > /data/exchange/jiezhixing02.txt


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  jiezhixing02;load data local infile '/data/exchange/jiezhixing02.txt' into table jiezhixing02 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 杰之行数据特殊处理  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 杰之行数据特殊处理   失败   ">> ${errologs}   
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST_2 -uroot -pPASS -D dugdb -e "
truncate table  all_account;
insert into all_account  select 'TIANMA' pt,orderid,sum(transactionamount) transactionamount from dugdb.tianma_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'IKOORI' pt,orderid,sum(transactionamount) transactionamount from dugdb.ikoori_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JIEZHIXING' pt,orderid,sum(transactionamount) transactionamount from dugdb.jiezhixing_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'WENSHI' pt,orderid,sum(transactionamount) transactionamount from dugdb.wenshi_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JIESITUO' pt,orderid,sum(transactionamount) transactionamount from dugdb.jiesituo_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'AOMEI' pt,orderid,sum(transactionamount) transactionamount from dugdb.aomei_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'WEIYI' pt,orderid,sum(transactionamount) transactionamount from dugdb.weiyi_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'YISHANG' pt,orderid,sum(transactionamount) transactionamount from dugdb.yishang_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'LIUSU' pt,orderid,sum(transactionamount) transactionamount from dugdb.liusu_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'GUOYU' pt,orderid,sum(transactionamount) transactionamount from dugdb.guoyu_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'YIWANG' pt,orderid,sum(transactionamount) transactionamount from dugdb.yiwang_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'SHANGDONG' pt,orderid,sum(transactionamount) transactionamount from dugdb.shangdong_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JIXIANG' pt,orderid,sum(transactionamount) transactionamount from dugdb.jixiang_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'YUTAI' pt,orderid,sum(transactionamount) transactionamount from dugdb.yutai_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'FEIFAN' pt,orderid,sum(transactionamount) transactionamount from dugdb.feifan_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JINLANG' pt,orderid,sum(transactionamount) transactionamount from dugdb.jinlang_account   where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'FAYA' pt,orderid,sum(transactionamount) transactionamount from dugdb.faya_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'FANXI' pt,orderid,sum(transactionamount) transactionamount from dugdb.fanxi_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'ZHONGSHENG' pt,orderid,sum(transactionamount) transactionamount from dugdb.zhongsheng_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JUYOUMEITE' pt,orderid,sum(transactionamount) transactionamount from dugdb.juyoumeite_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'BAOSHENG' pt,orderid,sum(transactionamount) transactionamount from dugdb.baosheng_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'DENGTENG' pt,orderid,sum(transactionamount) transactionamount from dugdb.dengteng_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'YIYAO' pt,orderid,sum(transactionamount) transactionamount from dugdb.yiyao_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'LIANHESHANGPIN' pt,orderid,sum(transactionamount) transactionamount from dugdb.lianheshangpin_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'SUGUAN' pt,orderid,sum(transactionamount) transactionamount from dugdb.suguan_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'CHENGMUSHANG' pt,orderid,sum(transactionamount) transactionamount from dugdb.chengmushang_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'GELINDAO' pt,orderid,sum(transactionamount) transactionamount from dugdb.gelindao_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'XIJIE' pt,orderid,sum(transactionamount) transactionamount from dugdb.xijie_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'DASHU' pt,orderid,sum(transactionamount) transactionamount from dugdb.dashu_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'HUDONG' pt,orderid,sum(transactionamount) transactionamount from dugdb.hudong_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'JIEZHINING' pt,orderid,sum(transactionamount) transactionamount from dugdb.jiezhining_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'BAIHONG' pt,orderid,sum(transactionamount) transactionamount from dugdb.baihong_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'QIAOLE' pt,orderid,sum(transactionamount) transactionamount from dugdb.qiaole_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'BAOFULAI' pt,orderid,sum(transactionamount) transactionamount from dugdb.baofulai_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'LIZHEN' pt,orderid,sum(transactionamount) transactionamount from dugdb.lizhen_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'MAISHENGYUEHE' pt,orderid,sum(transactionamount) transactionamount from dugdb.maishengyuehe_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'MINGXIEKU' pt,orderid,sum(transactionamount) transactionamount from dugdb.mingxieku_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'RUIDONG' pt,orderid,sum(transactionamount) transactionamount from dugdb.ruidong_account  where time >= '2026-01-01 00:00:00' group  by orderid;
insert into all_account  select 'HAILINGXUAN' pt,orderid,sum(transactionamount) transactionamount from dugdb.hailingxuan_account   group  by orderid;
insert into all_account  select 'HUJIAXING' pt,orderid,sum(transactionamount) transactionamount from dugdb.hujiaxing_account   group  by orderid;
insert into all_account  select 'GUFANZHURI' pt,orderid,sum(transactionamount) transactionamount from dugdb.gufanzhuri_account   group  by orderid;
insert into all_account  select 'XIEDUODUO' pt,关联订单号 orderid,sum(金额) transactionamount from dugdb.鞋多多账单表   group  by 关联订单号;


select * from all_account " > /data/exchange/all_account.txt


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  all_account;load data local infile '/data/exchange/all_account.txt' into table all_account character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

delete from all_account where transactionAmount = 0;
delete from all_account where orderid = '';
delete from all_account where orderid = 'NULL';

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " all_account  数据补充  成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " all_account  数据补充   失败   ">> ${errologs}   
    
fi



##########################################################################################################################
#中文描述：数据汇聚
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401
#############################################################################################################################
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;



-- 公司共鞋出货订单 source 使用的是自定义source  为了避免混淆  替换成so单号

update result_sale_order01 a,公司共鞋出货订单 b 
 set a.source = b.SO单号 
 where a.order_no=b.SO单号 ;



-- temu  订单source 统一
update result_sale_order01 set pickingsource ='TEMU' where  source like 'WB%';
update result_sale_order01 set source =substring_index(source,'-',1) where  source like 'WB%' and source like '%-%';




-- 将入库信息合并 然后删除重复so单号 数据  对应ag 
-- 将入库信息合并 然后删除重复so单号 数据  对应ag 
drop table if exists ruku_综合;
create table ruku_综合 as 
with test as (
select x.sale_name,group_concat(distinct x.mjBarcode separator'~') mjBarcode,sum(x.costprice) costprice,'入库' category,max(x.date_time) ruku_time from 
(select *,row_number() over (partition by source,mjbarcode order by date_time desc) sn from rugen_local_warehouse_ruku ) x  where x.sn = 1 
group by x.sale_name  union all 

select sale_name,group_concat(distinct mjBarcode separator'~') mjBarcode,max(costprice) costprice,'质量入库' category,max(date_time) ruku_time from rugen_local_warehouse_quality_problem group by sale_name union all 
select so_order_id,mjBarcode,0 costprice,'不明来源入库' category,createtime from 不明来源入库匹配 where so_order_id not in (select sale_name from rugen_local_warehouse_ruku) and so_order_id not in (select sale_name from rugen_local_warehouse_quality_problem)) 

select 
sale_name,
GROUP_CONCAT(distinct mjbarcode) mjBarcode,
max(costprice) costprice,
GROUP_CONCAT(distinct category) category,
max(ruku_time) ruku_time 

from test
group by sale_name
;

update ruku_综合 set category = '质量入库' where category like '%质量入库%'

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order02——01  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order02——01  失败   ">> ${errologs}   
    
fi







BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;


truncate table a_bh  ;  insert into a_bh  select xxxxx.so_order_id,group_concat(distinct xxxxx.orderid) orderid,sum(xxxxx.transactionamount) transactionamount from  (select distinct * from bigdata_mt.tiaohuo) xxxxx group by xxxxx.so_order_id ;

truncate table a_ah  ;  insert into a_ah  select sale_name,group_concat(distinct mjBarcode) mjBarcode,avg(costprice) costprice,'转寄' category,max(date_time) zhuanji_time from rugen_local_warehouse_zhuanji group by sale_name;
truncate table a_aah ;  insert into a_aah select name,group_concat(distinct express_number_to_samp separator'~') express_number_to_samp from zhuanji_total_to_qudao group by name;

truncate table a_ap  ;  
insert into a_ap  select max(date_time) zhuanji_return_time,sale_name,group_concat(distinct mjBarcode) zhuanji_return_mjbarcode,avg(costprice) costprice,'转寄退回' category,group_concat(distinct pickingExpressNumber separator'~') pickingExpressNumber 
from rugen_local_warehouse_zhuanji_return group by sale_name;

truncate table a_ak  ;  insert into a_ak  select so_order_id,substring_index(group_concat(express_no),',',1) express_no from rugen_cainiao_enter group by so_order_id;

truncate table a_aan ;  insert into a_aan select name,replace(group_concat(distinct express_number_to_samp),'-1','') express_number_to_samp from maijia_total_to_local where length(express_number_to_samp)<17 group by name;

truncate table a_at  ;  insert into a_at  select source,max(costprice) costprice from result_sale_order01 where sales_channels like '%批量退%' group by source;
"




if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order02——02  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order02——02  失败   ">> ${errologs}   
    
fi





BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;



truncate table result_sale_order02_bak01;
insert into result_sale_order02_bak01 


select 
DATEDIFF(a.date_time,al.date_time) ruku_datediff,
a.*,
00000000000.00 total_out,
00000000000.00 total_out_qudao,
-abs(at.costprice) piliangtui,

case when a.pickingsource='CAINIAO' and (ps_gx_order_state='发货完成' or ps_gx_order_state='仓库接单') then if(a.costprice=0,-abs(payamount),-abs(a.costprice)) end cainiao,
bb.transactionamount baoyuan,

case when a.pickingsource='LAOLU' and ps_gx_order_state='已发货' then -abs(a.costprice) end laolu,

-abs(fh.transactionamount) gongxie,
fg.transactionamount xijie,
ff.transactionamount qiaole,
fe.transactionamount jixiang,
fc.transactionamount suguan,    
fd.transactionamount weiyi,

fb.transactionamount jiezhining,    
fa.transactionamount gelindao,
ca.transactionamount lizhen,
cb.transactionamount yiwang,

bd.transactionamount aomei,
be.transactionamount liusu,
bf.transactionamount tongye,
bg.transactionamount zhongsheng,
bbg.transactionamount chengmushang,
-abs(bh.transactionamount) tiaohuo,
b.transactionamount baihong,
c.transactionamount baofulai,
d.transactionamount baosheng,
f.transactionamount changpao,
g.transactionamount dashu,
h.transactionamount dengteng,
i.transactionamount fanxi,
j.transactionamount faya,
k.transactionamount feifan,
l.transactionamount guoyu,
m.transactionamount hudong,
n.transactionamount jiesituo,
o.transactionamount jiezhixing,
p.transactionamount jinlang,
q.transactionamount juyoumeite,
r.transactionamount kurui,
s.transactionamount lianheshangpin,
t.transactionamount maishengyuehe,
u.transactionamount ruidong,
v.transactionamount shangdong,
w.transactionamount tianma,
x.transactionamount wenshi,
y.transactionamount wuzhe,
aa.transactionamount yishang,
ab.transactionamount yiyao,
ac.transactionamount yutai,

ad.costprice local_warehouse,
ad.date_time local_warehouse_chuhuo_time,
ae.amount order_3150,
ae.payment_type,
af.supplementary_amount,
ag.ruku_time local_warehouse_ruku_time,
ag.category local_warehouse_category,
ag.sale_name local_warehouse_so,
ag.mjBarcode local_warehouse_mjbarcode,
ag.costprice local_warehouse_costprice,
ah.zhuanji_time zhuanji_time,
ah.category zhuanji_category,
ah.costprice zhuanji_costprice,
ah.mjBarcode zhuanji_mjBarcode,
aah.express_number_to_samp zhuanji_pickingExpressNumber,
ap.zhuanji_return_time,
ap.zhuanji_return_mjbarcode,
ap.category zhuanji_return_category,
ap.costprice zhuanji_return_costprice,
ap.pickingExpressNumber zhuanji_return_pickingExpressNumber,
ak.so_order_id  cainiao_category,
0 cainiao_costprice,
am.so_order_id cainiao_zhuanji_category,
0 cainiao_zhuanji,
an.refund_status,
an.good_status,
an.max_refund_amount,
an.refund_local,

aan.express_number_to_samp refund_express_number,

an.weifamiaotui,
al.date_time mjBarcode_ruku_date,
ao.date_time mjBarcode_lastest_entertime,
ao.pickingExpressNumber lastest_pickingExpressNumber,
bb.orderid baoyuan_num,
'' laolu_num,
fh.orderid gogxietiaohuo_num,
fg.orderid xijie_num,
ff.orderid qiaole_num,
fe.orderid jixiang_num,
fc.orderid suguan_num,
fd.orderid weiyi_num,

fb.orderid jiezhining_num,
fa.orderid gelindao_num,
ca.orderid lizhen_num,
cb.orderid yiwang_num,
bd.orderid aomei_num,
be.orderid liusu_num,
bf.orderid tongye_num,
bg.orderid zhongsheng_num,
bbg.orderid chengmushang_num,
bh.orderid tiaohuo_num,
b.orderid baihong_num,
c.orderid baofulai_num,
d.orderid baosheng_num,
f.orderid changpao_num,
g.orderid dashu_num,
h.orderid dengteng_num,
i.orderid fanxi_num,
j.orderid faya_num,
k.orderid feifan_num,
l.orderid guoyu_num,
m.orderid hudong_num,
n.orderid jiesituo_num,
o.orderid jiezhixing_num,
p.orderid jinlang_num,
q.orderid juyoumeite_num,
r.orderid kurui_num,
s.orderid lianheshangpin_num,
t.orderid maishengyuehe_num,
u.orderid ruidong_num,
v.orderid shangdong_num,
w.orderid tianma_num,
x.orderid wenshi_num,
y.orderid wuzhe_num,
aa.orderid yishang_num,
ab.orderid yiyao_num,
ac.orderid yutai_num,

ad.mjbarcode local_warehouse_num 

from result_sale_order01 a 

left join 共鞋调货 fh on a.order_no = fh.so_order_id
left join bigdata_mt.xijie01 fg on a.order_no = fg.so_order_id
left join bigdata_mt.qiaole01 ff on a.order_no = ff.so_order_id
left join bigdata_mt.suguan01 fc on a.order_no = fc.so_order_id
left join bigdata_mt.weiyi01 fd on a.order_no = fd.so_order_id
left join bigdata_mt.jixiang01 fe on a.order_no = fe.so_order_id
left join bigdata_mt.jiezhining01 fb on a.order_no = fb.so_order_id
left join bigdata_mt.gelindao01 fa on a.order_no = fa.so_order_id
left join bigdata_mt.lizhen01 ca on a.order_no = ca.so_order_id  
left join bigdata_mt.yiwang01 cb on a.order_no = cb.so_order_id 
left join bigdata_mt.baoyuan01 bb on a.order_no = bb.so_order_id  
left join bigdata_mt.aomei01 bd on a.order_no = bd.so_order_id 
left join bigdata_mt.liusu01 be on a.order_no = be.so_order_id 
left join rugen.ty_02 bf on a.order_no = bf.so_order_id 
left join bigdata_mt.zhongsheng01 bg on a.order_no = bg.so_order_id
left join bigdata_mt.chengmushang01 bbg on a.order_no = bbg.so_order_id
left join a_bh bh on a.order_no = bh.so_order_id 
left join bigdata_mt.baihong01 b on a.order_no = b.so_order_id 
left join bigdata_mt.baofulai01 c on a.order_no = c.so_order_id 
left join bigdata_mt.baosheng01 d on a.order_no = d.so_order_id 
left join bigdata_mt.changpao01  f on a.order_no = f.so_order_id 
left join bigdata_mt.dashu01  g on a.order_no = g.so_order_id 
left join bigdata_mt.dengteng01  h on a.order_no = h.so_order_id 
left join bigdata_mt.fanxi01  i on a.order_no = i.so_order_id 
left join bigdata_mt.faya01  j on a.order_no = j.so_order_id 
left join bigdata_mt.feifan01  k on a.order_no = k.so_order_id 
left join bigdata_mt.guoyu01  l on a.order_no = l.so_order_id 
left join bigdata_mt.hudong01  m on a.order_no = m.so_order_id 
left join bigdata_mt.jiesituo01  n on a.order_no = n.so_order_id 
left join bigdata_mt.jiezhixing01  o on a.order_no = o.so_order_id 
left join bigdata_mt.jinlang01  p on a.order_no = p.so_order_id 
left join bigdata_mt.juyoumeite01  q on a.order_no = q.so_order_id 
left join bigdata_mt.kurui01  r on a.order_no = r.so_order_id 
left join bigdata_mt.lianheshangpin01  s on a.order_no = s.so_order_id 
left join bigdata_mt.maishengyuehe01  t on a.order_no = t.so_order_id 
left join bigdata_mt.ruidong01  u on a.order_no = u.so_order_id 
left join bigdata_mt.shangdong01  v on a.order_no = v.so_order_id 
left join bigdata_mt.tianma01  w on a.order_no = w.so_order_id 
left join bigdata_mt.wenshi01  x on a.order_no = x.so_order_id 
left join bigdata_mt.wuzhe01  y on a.order_no = y.so_order_id 
left join bigdata_mt.yishang01  aa on a.order_no = aa.so_order_id 
left join bigdata_mt.yiyao01  ab on a.order_no = ab.so_order_id 
left join bigdata_mt.yutai01  ac on a.order_no = ac.so_order_id 

left join local_warehouse01  ad on a.order_no = ad.so_order_id 
left join result_sale_order_3150  ae on a.order_no = ae.so_name  
left join result_sale_order_supplement  af on a.order_no = af.so_name  
left join ruku_综合 ag on a.order_no=ag.sale_name 
left join a_ah ah on a.order_no=ah.sale_name 
left join a_aah aah on a.order_no=aah.name  
left join a_ap ap on a.order_no=ap.sale_name
left join a_ak ak on a.order_no=ak.so_order_id 
left join rugen_cainiao_zhuanji am on a.order_no=am.so_order_id 
left join result_sale_order_after_sales_total01 an on a.order_no=an.name 
left join a_aan aan on a.order_no=aan.name 
left join local_warehouse_mjbarcode01 al on a.ps_gx_order_no=al.mjBarcode
left join local_warehouse_mjbarcode_lastest_entertime ao on a.ps_gx_order_no=ao.mjBarcode
left join a_at at on a.order_no = at.source 
;


truncate table result_sale_order02_bak02;
insert into result_sale_order02_bak02 
select 
a.order_no,
c11.transactionamount diwuji,
c12.transactionamount mingxieku,
c13.transactionamount bien,
c14.transactionamount naichuang,
c15.transactionamount bingoujicang,
c16.transactionamount tupoyundong,
c17.transactionamount hailingxuan,
c18.transactionamount hujiaxing,
c19.transactionamount gufanzhuri,
c20.transactionamount qingrui,
c21.transactionamount yuge,
c22.transactionamount heishi,
c23.transactionamount lingxian,
c24.transactionamount chengziyundong,
c25.transactionamount weijiucheng,
c26.transactionamount quanyong,
c27.transactionamount weihaisibozi,
c28.transactionamount qutao,
c29.transactionamount lingchilong,
c30.transactionamount delijuchuan,
c31.transactionamount xingyueaotelaisi,
c32.transactionamount gongxie_myjochuku,
c33.transactionamount jixiangtuangou,
c11.orderid diwuji_num,
c12.orderid mingxieku_num,
c13.orderid bien_num,
c14.orderid naichuang_num,
c15.orderid bingoujicang_num,
c16.orderid tupoyundong_num,
c17.orderid hailingxuan_num,
c18.orderid hujiaxing_num,
c19.orderid gufanzhuri_num,
c20.orderid qingrui_num,
c21.orderid yuge_num,
c22.orderid heishi_num,
c23.orderid lingxian_num,
c24.orderid chengziyundong_num,
c25.orderid weijiucheng_num,
c26.orderid quanyong_num,
c27.orderid weihaisibozi_num,
c28.orderid qutao_num,
c29.orderid lingchilong_num,
c30.orderid delijuchuan_num,
c31.orderid xingyueaotelaisi_num,
c32.orderid gongxie_myjochuku_num,
c33.orderid jixiangtuangou_num 


from result_sale_order01 a 


left join bigdata_mt.diwuji01 c11 on a.order_no = c11.so_order_id 
left join bigdata_mt.mingxieku01 c12 on a.order_no = c12.so_order_id 
left join bigdata_mt.bien01 c13 on a.order_no = c13.so_order_id 
left join bigdata_mt.naichuang01 c14 on a.order_no = c14.so_order_id 
left join bigdata_mt.bingoujicang01 c15 on a.order_no = c15.so_order_id 
left join bigdata_mt.tupoyundong01 c16 on a.order_no = c16.so_order_id 
left join bigdata_mt.hailingxuan01 c17 on a.order_no = c17.so_order_id 
left join bigdata_mt.hujiaxing01 c18 on a.order_no = c18.so_order_id 
left join bigdata_mt.gufanzhuri01 c19 on a.order_no = c19.so_order_id 
left join bigdata_mt.qingrui01 c20 on a.order_no = c20.so_order_id 
left join bigdata_mt.yuge01 c21 on a.order_no = c21.so_order_id 
left join bigdata_mt.heishi01 c22 on a.order_no = c22.so_order_id 
left join bigdata_mt.lingxian01 c23 on a.order_no = c23.so_order_id 
left join bigdata_mt.chengziyundong01 c24 on a.order_no = c24.so_order_id 
left join bigdata_mt.weijiucheng01 c25 on a.order_no = c25.so_order_id 
left join bigdata_mt.quanyong01 c26 on a.order_no = c26.so_order_id 
left join bigdata_mt.weihaisibozi01 c27 on a.order_no = c27.so_order_id 
left join bigdata_mt.qutao01 c28 on a.order_no = c28.so_order_id 
left join bigdata_mt.lingchilong01 c29 on a.order_no = c29.so_order_id 
left join bigdata_mt.delijuchuan01 c30 on a.order_no = c30.so_order_id 
left join bigdata_mt.xingyueaotelaisi01 c31 on a.order_no = c31.so_order_id 
left join bigdata_mt.gongxie_myjochuku01 c32 on a.order_no = c32.so_order_id 
left join bigdata_mt.jixiangtuangou01 c33 on a.order_no = c33.so_order_id 

;




truncate table result_sale_order02;
insert into result_sale_order02 

select 
a.*,
b.diwuji, 
b.mingxieku, 
b.bien, 
b.naichuang, 
b.bingoujicang, 
b.tupoyundong, 
b.hailingxuan, 
b.hujiaxing, 
b.gufanzhuri, 
b.qingrui, 
b.yuge, 
b.heishi, 
b.lingxian, 
b.chengziyundong, 
b.weijiucheng, 
b.quanyong, 
b.weihaisibozi, 
b.qutao, 
b.lingchilong, 
b.delijuchuan, 
b.xingyueaotelaisi, 
b.gongxie_myjochuku, 
b.jixiangtuangou, 
b.diwuji_num, 
b.mingxieku_num, 
b.bien_num, 
b.naichuang_num, 
b.bingoujicang_num, 
b.tupoyundong_num, 
b.hailingxuan_num, 
b.hujiaxing_num, 
b.gufanzhuri_num, 
b.qingrui_num, 
b.yuge_num, 
b.heishi_num, 
b.lingxian_num, 
b.chengziyundong_num, 
b.weijiucheng_num, 
b.quanyong_num, 
b.weihaisibozi_num, 
b.qutao_num, 
b.lingchilong_num, 
b.delijuchuan_num, 
b.xingyueaotelaisi_num, 
b.gongxie_myjochuku_num, 
b.jixiangtuangou_num 
from result_sale_order02_bak01 a 
left join result_sale_order02_bak02 b 
on a.order_no = b.order_no;

"
if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order02  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order02  失败   ">> ${errologs}   
    
fi





BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "



-- 共鞋-销售渠道 是人工在线表格导入 和 本地仓出库重合  需要单独处理  现在得处理方法就是 二取一
update result_sale_order02 set local_warehouse= 0 where sales_channels = '共鞋-销售渠道' and gongxie_myjochuku is not null;



-- update result_sale_order02 set piliangtui =0 where piliangtui is null;
-- update result_sale_order02 set cainiao =0 where cainiao is null;
-- update result_sale_order02 set baoyuan =0 where baoyuan is null;
-- update result_sale_order02 set laolu =0 where laolu is null;
-- update result_sale_order02 set gongxie =0 where gongxie is null;
-- update result_sale_order02 set xijie =0 where xijie is null;
-- update result_sale_order02 set qiaole =0 where qiaole is null;
-- update result_sale_order02 set jixiang =0 where jixiang is null;
-- update result_sale_order02 set suguan =0 where suguan is null;
-- update result_sale_order02 set weiyi =0 where weiyi is null;
-- update result_sale_order02 set jiezhining =0 where jiezhining is null;
-- update result_sale_order02 set gelindao =0 where gelindao is null;
-- update result_sale_order02 set lizhen =0 where lizhen is null;
-- update result_sale_order02 set yiwang =0 where yiwang is null;
-- update result_sale_order02 set aomei =0 where aomei is null;
-- update result_sale_order02 set liusu =0 where liusu is null;
-- update result_sale_order02 set tongye =0 where tongye is null;
-- update result_sale_order02 set zhongsheng =0 where zhongsheng is null;
-- update result_sale_order02 set chengmushang =0 where chengmushang is null;
-- update result_sale_order02 set tiaohuo =0 where tiaohuo is null;
-- update result_sale_order02 set baihong =0 where baihong is null;
-- update result_sale_order02 set baofulai =0 where baofulai is null;
-- update result_sale_order02 set baosheng =0 where baosheng is null;
-- update result_sale_order02 set changpao =0 where changpao is null;
-- update result_sale_order02 set dashu =0 where dashu is null;
-- update result_sale_order02 set dengteng =0 where dengteng is null;
-- update result_sale_order02 set fanxi =0 where fanxi is null;
-- update result_sale_order02 set faya =0 where faya is null;
-- update result_sale_order02 set feifan =0 where feifan is null;
-- update result_sale_order02 set guoyu =0 where guoyu is null;
-- update result_sale_order02 set hudong =0 where hudong is null;
-- update result_sale_order02 set jiesituo =0 where jiesituo is null;
-- update result_sale_order02 set jiezhixing =0 where jiezhixing is null;
-- update result_sale_order02 set jinlang =0 where jinlang is null;
-- update result_sale_order02 set juyoumeite =0 where juyoumeite is null;
-- update result_sale_order02 set kurui =0 where kurui is null;
-- update result_sale_order02 set lianheshangpin =0 where lianheshangpin is null;
-- update result_sale_order02 set maishengyuehe =0 where maishengyuehe is null;
-- update result_sale_order02 set ruidong =0 where ruidong is null;
-- update result_sale_order02 set shangdong =0 where shangdong is null;
-- update result_sale_order02 set tianma =0 where tianma is null;
-- update result_sale_order02 set wenshi =0 where wenshi is null;
-- update result_sale_order02 set wuzhe =0 where wuzhe is null;
-- update result_sale_order02 set yishang =0 where yishang is null;
-- update result_sale_order02 set yiyao =0 where yiyao is null;
-- update result_sale_order02 set yutai =0 where yutai is null;
-- update result_sale_order02 set local_warehouse =0 where local_warehouse is null;




-- 杰之行订单表 externalOrderNo 抖店订单 部分不是SO  是source  需要 单独更新杰之行 支出

update result_sale_order02 a ,jiezhixing02 b 
set a.jiezhixing = b.transactionAmount
where a.ps_gx_order_no=b.orderid;

update result_sale_order02 a,all_account b set a.ruidong=b.transactionamount where a.pickingSource='XIEDUODUO' and a.ruidong is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.dashu=b.transactionamount where a.pickingSource='DASHU' and a.dashu is null and a.ps_gx_order_no=b.orderid;


update result_sale_order02 a,all_account b set a.tianma=b.transactionamount where a.pickingSource='TIANMA' and a.tianma is null  and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.kurui=b.transactionamount where a.pickingSource='IKOORI' and a.kurui is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.jiezhixing=b.transactionamount where a.pickingSource='JIEZHIXING' and a.jiezhixing is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.wenshi=b.transactionamount where a.pickingSource='WENSHI' and a.wenshi is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.jiesituo=b.transactionamount where a.pickingSource='JIESITUO' and a.jiesituo is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.aomei=b.transactionamount where a.pickingSource='AOMEI' and a.aomei is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.weiyi=b.transactionamount where a.pickingSource='WEIYI' and a.weiyi is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.yishang=b.transactionamount where a.pickingSource='YISHANG' and a.yishang is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.liusu=b.transactionamount where a.pickingSource='LIUSU' and a.liusu is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.guoyu=b.transactionamount where a.pickingSource='GUOYU' and a.guoyu is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.yiwang=b.transactionamount where a.pickingSource='YIWANG' and a.yiwang is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.shangdong=b.transactionamount where a.pickingSource='SHANGDONG' and a.shangdong is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.jixiang=b.transactionamount where a.pickingSource='JIXIANG' and a.jixiang is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.yutai=b.transactionamount where a.pickingSource='YUTAI' and a.yutai is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.feifan=b.transactionamount where a.pickingSource='FEIFAN' and a.feifan is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.jinlang=b.transactionamount where a.pickingSource='JINLANG' and a.jinlang is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.faya=b.transactionamount where a.pickingSource='FAYA' and a.faya is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.fanxi=b.transactionamount where a.pickingSource='FANXI' and a.fanxi is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.zhongsheng=b.transactionamount where a.pickingSource='ZHONGSHENG' and a.zhongsheng is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.juyoumeite=b.transactionamount where a.pickingSource='JUYOUMEITE' and a.juyoumeite is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.baosheng=b.transactionamount where a.pickingSource='BAOSHENG' and a.baosheng is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.dengteng=b.transactionamount where a.pickingSource='DENGTENG' and a.dengteng is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.yiyao=b.transactionamount where a.pickingSource='YIYAO' and a.yiyao is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.lianheshangpin=b.transactionamount where a.pickingSource='LIANHESHANGPIN' and a.lianheshangpin is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.suguan=b.transactionamount where a.pickingSource='SUGUAN' and a.suguan is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.chengmushang=b.transactionamount where a.pickingSource='CHENGMUSHANG' and a.chengmushang is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.gelindao=b.transactionamount where a.pickingSource='GELINDAO' and a.gelindao is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.xijie=b.transactionamount where a.pickingSource='XIJIE' and a.xijie is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.hudong=b.transactionamount where a.pickingSource='HUDONG' and a.hudong is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.jiezhining=b.transactionamount where a.pickingSource='JIEZHINING' and a.jiezhining is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.baihong=b.transactionamount where a.pickingSource='BAIHONG' and a.baihong is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.qiaole=b.transactionamount where a.pickingSource='QIAOLE' and a.qiaole is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.baofulai=b.transactionamount where a.pickingSource='BAOFULAI' and a.baofulai is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.lizhen=b.transactionamount where a.pickingSource='LIZHEN' and a.lizhen is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.maishengyuehe=b.transactionamount where a.pickingSource='MAISHENGYUEHE' and a.maishengyuehe is null and a.ps_gx_order_no=b.orderid;
update result_sale_order02 a,all_account b set a.ruidong=b.transactionamount where a.pickingSource='RUIDONG' and a.ruidong is null and a.ps_gx_order_no=b.orderid;




update result_sale_order02  set tianma_num=ps_gx_order_no where pickingSource='TIANMA' and tianma is not null  and tianma_num   is null;
update result_sale_order02  set kurui_num=ps_gx_order_no where pickingSource='IKOORI' and kurui is not null and   kurui_num   is null;
update result_sale_order02  set jiezhixing_num=ps_gx_order_no where pickingSource='JIEZHIXING' and jiezhixing is not null  and jiezhixing_num  is null;
update result_sale_order02  set wenshi_num=ps_gx_order_no where pickingSource='WENSHI' and wenshi is not null  and wenshi_num  is null;
update result_sale_order02  set jiesituo_num=ps_gx_order_no where pickingSource='JIESITUO' and jiesituo is not null   and jiesituo_num  is null;
update result_sale_order02  set aomei_num=ps_gx_order_no where pickingSource='AOMEI' and aomei is not null and  aomei_num  is null;
update result_sale_order02  set weiyi_num=ps_gx_order_no where pickingSource='WEIYI' and weiyi is not null and  weiyi_num is null;
update result_sale_order02  set yishang_num=ps_gx_order_no where pickingSource='YISHANG' and yishang is not null and  yishang_num is null;
update result_sale_order02  set liusu_num=ps_gx_order_no where pickingSource='LIUSU' and liusu is not null and  liusu_num is null;
update result_sale_order02  set guoyu_num=ps_gx_order_no where pickingSource='GUOYU' and guoyu is not null and  guoyu_num is null;
update result_sale_order02  set yiwang_num=ps_gx_order_no where pickingSource='YIWANG' and yiwang is not null and  yiwang_num is null;
update result_sale_order02  set shangdong_num=ps_gx_order_no where pickingSource='SHANGDONG' and shangdong is not null and  shangdong_num is null;
update result_sale_order02  set jixiang_num=ps_gx_order_no where pickingSource='JIXIANG' and jixiang is not null and  jixiang_num is null;
update result_sale_order02  set yutai_num=ps_gx_order_no where pickingSource='YUTAI' and yutai is not null and  yutai_num is null;
update result_sale_order02  set feifan_num=ps_gx_order_no where pickingSource='FEIFAN' and feifan is not null and  feifan_num is null;
update result_sale_order02  set jinlang_num=ps_gx_order_no where pickingSource='JINLANG' and jinlang is not null and  jinlang_num is null;
update result_sale_order02  set faya_num=ps_gx_order_no where pickingSource='FAYA' and faya is not null and  faya_num is null;
update result_sale_order02  set fanxi_num=ps_gx_order_no where pickingSource='FANXI' and fanxi is not null and  fanxi_num is null;
update result_sale_order02  set zhongsheng_num=ps_gx_order_no where pickingSource='ZHONGSHENG' and zhongsheng is not null and  zhongsheng_num is null;
update result_sale_order02  set juyoumeite_num=ps_gx_order_no where pickingSource='JUYOUMEITE' and juyoumeite is not null and  juyoumeite_num is null;
update result_sale_order02  set baosheng_num=ps_gx_order_no where pickingSource='BAOSHENG' and baosheng is not null and  baosheng_num is null;
update result_sale_order02  set dengteng_num=ps_gx_order_no where pickingSource='DENGTENG' and dengteng is not null and  dengteng_num is null;
update result_sale_order02  set yiyao_num=ps_gx_order_no where pickingSource='YIYAO' and yiyao is not null and  yiyao_num is null;
update result_sale_order02  set lianheshangpin_num=ps_gx_order_no where pickingSource='LIANHESHANGPIN' and lianheshangpin is not null and  lianheshangpin_num is null;
update result_sale_order02  set suguan_num=ps_gx_order_no where pickingSource='SUGUAN' and suguan is not null and  suguan_num is null;
update result_sale_order02  set chengmushang_num=ps_gx_order_no where pickingSource='CHENGMUSHANG' and chengmushang is not null and  chengmushang_num is null;
update result_sale_order02  set gelindao_num=ps_gx_order_no where pickingSource='GELINDAO' and gelindao is not null and  gelindao_num is null;
update result_sale_order02  set xijie_num=ps_gx_order_no where pickingSource='XIJIE' and xijie is not null and  xijie_num is null;
update result_sale_order02  set dashu_num=ps_gx_order_no where pickingSource='DASHU' and dashu is not null and  dashu_num is null;
update result_sale_order02  set hudong_num=ps_gx_order_no where pickingSource='HUDONG' and hudong is not null and  hudong_num is null;
update result_sale_order02  set jiezhining_num=ps_gx_order_no where pickingSource='JIEZHINING' and jiezhining is not null and  jiezhining_num is null;
update result_sale_order02  set baihong_num=ps_gx_order_no where pickingSource='BAIHONG' and baihong is not null and  baihong_num is null;
update result_sale_order02  set qiaole_num=ps_gx_order_no where pickingSource='QIAOLE' and qiaole is not null and  qiaole_num is null;
update result_sale_order02  set baofulai_num=ps_gx_order_no where pickingSource='BAOFULAI' and baofulai is not null and  baofulai_num is null;
update result_sale_order02  set lizhen_num=ps_gx_order_no where pickingSource='LIZHEN' and lizhen is not null and  lizhen_num is null;
update result_sale_order02  set maishengyuehe_num=ps_gx_order_no where pickingSource='MAISHENGYUEHE' and maishengyuehe is not null and  maishengyuehe_num is null;
update result_sale_order02  set ruidong_num=ps_gx_order_no where pickingSource='RUIDONG' and ruidong is not null and  ruidong_num is null;





-- 补充批量退回渠道 订单金额

update result_sale_order02 set piliangtui = 0 where piliangtui is null;


-- 补充本地仓出库订单  成本价  
update result_sale_order02 set local_warehouse=costprice where local_warehouse_num is not null and local_warehouse =0;
update result_sale_order02 set local_warehouse=payAmount where local_warehouse_num is not null and local_warehouse =0;

-- 补充不明来源入库成本价

update result_sale_order02 set local_warehouse_costprice = costprice where local_warehouse_category = '不明来源入库' and local_warehouse_costprice=0;
update result_sale_order02 set local_warehouse_costprice = payamount where local_warehouse_category = '不明来源入库' and local_warehouse_costprice=0;






-- 补充本仓出库订单 入库信息

update result_sale_order02 
set 
local_warehouse_category='入库',
local_warehouse_so=order_no,
local_warehouse_costprice=costprice,
local_warehouse_mjbarcode=zhuanji_mjbarcode,
local_warehouse_ruku_time=zhuanji_time  
where local_warehouse_category is null and zhuanji_category is not null and platform_system_number = '单';


update result_sale_order02 
set 
local_warehouse_ruku_time=mjBarcode_lastest_entertime,
local_warehouse_category='入库',
local_warehouse_so=order_no,
local_warehouse_mjbarcode=local_warehouse_num,
local_warehouse_costprice=local_warehouse 
where 
TIMESTAMPDIFF(MINUTE,local_warehouse_chuhuo_time,mjBarcode_lastest_entertime)>0 and 
local_warehouse_chuhuo_time is not null and local_warehouse_ruku_time is null ;



update result_sale_order02 set zhuanji_pickingExpressNumber = '' where zhuanji_pickingExpressNumber is null;
update result_sale_order02 set zhuanji_pickingExpressNumber = '' where zhuanji_pickingExpressNumber != '' and zhuanji_category is null;

-- 如果 转寄退回  有记录  但是没有  转寄记录   用下面语句 补充  转寄信息
update result_sale_order02 set zhuanji_category='转寄',zhuanji_costprice = costprice,zhuanji_time = local_warehouse_ruku_time 
where zhuanji_category is null and zhuanji_return_category = '转寄退回' and local_warehouse_category is not null;


-- 补充最大入库时间处理
update result_sale_order02 a,local_warehouse_mjbarcode_lastest_entertime b 
set a.mjbarcode_lastest_entertime=b.date_time 
where  a.local_warehouse_mjbarcode=b.mjbarcode and a.mjbarcode_lastest_entertime is null and pickingSource != 'MYJO' ;

update result_sale_order02 a,local_warehouse_mjbarcode_lastest_entertime b 
set a.mjbarcode_lastest_entertime=b.date_time 
where a.local_warehouse_mjbarcode=b.mjBarcode and a.local_warehouse_mjbarcode is not null and a.mjbarcode_lastest_entertime is null;


-- 如果公司码最大入库时间 大于转寄时间  那么 就用下面语句补充  转寄退回  信息

update result_sale_order02 
set zhuanji_return_time=mjBarcode_lastest_entertime,zhuanji_return_category='转寄退回',zhuanji_return_costprice=costprice,zhuanji_return_mjbarcode=local_warehouse_mjbarcode,zhuanji_return_pickingExpressNumber=zhuanji_pickingExpressNumber 
where 
TIMESTAMPDIFF(MINUTE,zhuanji_time,mjBarcode_lastest_entertime)>0 and pickingSource != 'MYJO'
;

-- 如果本地仓 发货  渠道是  myjo    没有出库成本    却有  入库信息       那么入库信息 清空
update result_sale_order02 set local_warehouse_category = '',local_warehouse_so = '', local_warehouse_mjbarcode = '',local_warehouse_costprice = 0 
where pickingSource = 'MYJO' and local_warehouse is null and local_warehouse_costprice !=0;

update result_sale_order02 set local_warehouse_category = '',local_warehouse_so = '', local_warehouse_mjbarcode = '',local_warehouse_costprice = 0 
where pickingSource = 'MYJO' and local_warehouse_category not like '%入库%' and local_warehouse_costprice >0 ;



-- 共鞋-销售渠道   天猫麦稻负责   成本控制在 乔乐平台里面    和本地仓支出 重合 需要处理

update result_sale_order02 set qiaole = local_warehouse where sales_channels = '共鞋-销售渠道' and qiaole  is null;
update result_sale_order02 set local_warehouse = 0  where sales_channels = '共鞋-销售渠道' and qiaole  is not null;




-- 特殊备注  雨歌批量退 11.21 批量退了163件货   直播间款   退款对应得ERP订单是  11.14号   sales_channels 是  三分球-批量退   

update result_sale_order02 

set local_warehouse = -10 , total_out = -10 ,total_out_qudao = -10 

where sales_channels = '三分球-批量退' and date_time like '2025-11-14%' ;



"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order02数据更新  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order02数据更新  失败   ">> ${errologs}   
    
fi






BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;


ALTER TABLE result_sale_order02 MODIFY total_out DECIMAL(12,2);
ALTER TABLE result_sale_order02 MODIFY total_out_qudao DECIMAL(12,2);

update result_sale_order02 set local_warehouse = -abs(local_warehouse) ;

update result_sale_order02 
set 
local_warehouse_ruku_time=mjBarcode_lastest_entertime,
local_warehouse_category='',
local_warehouse_so='',
local_warehouse_mjbarcode='',
local_warehouse_costprice=0 
where pickingSource='MYJO' and local_warehouse_num is null and local_warehouse_category like '%入库%';





update result_sale_order02 set local_warehouse_costprice = costprice where local_warehouse_costprice=0 and local_warehouse_category = '入库';
update result_sale_order02 set local_warehouse_costprice = payAmount where local_warehouse_costprice=0 and local_warehouse_category = '入库';

update result_sale_order02 set local_warehouse_costprice = costprice where local_warehouse_costprice=0 and local_warehouse_category = '质量入库';
update result_sale_order02 set local_warehouse_costprice = payAmount where local_warehouse_costprice=0 and local_warehouse_category = '质量入库';

update result_sale_order02 set local_warehouse_costprice = costprice where local_warehouse_costprice=0 and local_warehouse_category = '不明来源入库';
update result_sale_order02 set local_warehouse_costprice = payAmount where local_warehouse_costprice=0 and local_warehouse_category = '不明来源入库';


-- 本地仓出库实际支出 更新    本地仓出库价格+ 入库价格  备注：本地仓不存在入库 如果入库就是  成本支出 为0 
update result_sale_order02 set local_warehouse_costprice =0 where pickingSource = 'MYJO' and local_warehouse_costprice is null ;
update result_sale_order02 set local_warehouse = 0 where pickingSource = 'MYJO' and local_warehouse is null;



-- 这里增加四个 更新语句作用就是   本地仓出库后 又入库   这种不能算出库   本地仓  local_warehouse  只算真实出库  本地仓不存在入库   转寄   转寄退回sd

update result_sale_order02 
set local_warehouse_costprice = local_warehouse +local_warehouse_costprice,zhuanji_costprice = 0,zhuanji_return_costprice=0 
where pickingSource = 'MYJO' AND local_warehouse_category like '%入库%';


update result_sale_order02 
set local_warehouse = 0
where pickingSource = 'MYJO' AND local_warehouse_category like '%入库%';



update result_sale_order02 
set local_warehouse_costprice = 0 
where pickingSource = 'MYJO' AND local_warehouse_category like '%入库%' and local_warehouse_costprice !=0 and platform_system_number = '单';


update result_sale_order02 
set local_warehouse_costprice = 0 
where pickingSource = 'MYJO' AND local_warehouse_category like '%入库%'  and local_warehouse_costprice <0;













update result_sale_order02 set total_out_qudao=ifnull(gongxie,0) +  ifnull(xijie,0) +  ifnull(qiaole,0) +  ifnull(jixiang,0) + ifnull(suguan,0) + ifnull(weiyi,0) +ifnull(jiezhining,0) + ifnull(gelindao,0) + ifnull(lizhen,0) + ifnull(yiwang,0) + ifnull(chengmushang,0) + ifnull(tiaohuo,0) + ifnull(zhongsheng,0) + ifnull(tongye,0) + ifnull(laolu,0) + ifnull(aomei,0) + ifnull(liusu,0) + ifnull(cainiao,0) + ifnull(baoyuan,0) + ifnull(baihong,0) + ifnull(baofulai,0) + ifnull(baosheng,0) + ifnull(changpao,0) + ifnull(dashu,0) + ifnull(dengteng,0) + ifnull(fanxi,0) + ifnull(faya,0) + ifnull(feifan,0) + ifnull(guoyu,0) + ifnull(hudong,0) + ifnull(jiesituo,0) + ifnull(jiezhixing,0) + ifnull(jinlang,0) + ifnull(juyoumeite,0) + ifnull(kurui,0) + ifnull(lianheshangpin,0) + ifnull(maishengyuehe,0) + ifnull(ruidong,0) + ifnull(shangdong,0) + ifnull(tianma,0) + ifnull(wenshi,0) + ifnull(wuzhe,0) + 
ifnull(yishang,0) + 
ifnull(yiyao,0) + 
ifnull(yutai,0) + 
ifnull(diwuji,0) + 
ifnull(mingxieku,0) + 
ifnull(bien,0) + 
ifnull(naichuang,0) + 
ifnull(bingoujicang,0) + 
ifnull(tupoyundong,0) + 
ifnull(hailingxuan,0) + 
ifnull(hujiaxing,0) + 
ifnull(gufanzhuri,0) + 
ifnull(qingrui,0) + 
ifnull(yuge,0) + 
ifnull(heishi,0) + 
ifnull(lingxian,0) + 
ifnull(chengziyundong,0) + 
ifnull(weijiucheng,0) + 
ifnull(quanyong,0) + 
ifnull(weihaisibozi,0) + 
ifnull(qutao,0) + 
ifnull(lingchilong,0) + 
ifnull(delijuchuan,0) + 
ifnull(xingyueaotelaisi,0) + 
ifnull(gongxie_myjochuku,0) + 
ifnull(jixiangtuangou,0) + 
ifnull(local_warehouse,0);

update result_sale_order02 set cainiao_costprice=costprice where cainiao_category like 'so%';
update result_sale_order02 set cainiao_costprice=payAmount where cainiao_category like 'so%' and cainiao_costprice=0;


update result_sale_order02 set cainiao_zhuanji=costprice where cainiao_zhuanji_category like 'so%';
update  result_sale_order02 set cainiao_costprice = cainiao_zhuanji where cainiao_zhuanji >5 and cainiao_costprice = 0;


update result_sale_order02 set zhuanji_costprice = costprice where zhuanji_costprice=0 and zhuanji_category = '转寄' and pickingsource != 'MYJO';
update result_sale_order02 set zhuanji_costprice = local_warehouse_costprice where zhuanji_costprice=0 and zhuanji_category = '转寄' and pickingsource != 'MYJO';



update result_sale_order02 set zhuanji_return_costprice = costprice where zhuanji_return_costprice=0 and zhuanji_return_category = '转寄退回' and pickingsource != 'MYJO';



-- 这一步是计算  实际支出    平台扣款 + 入库 - 转寄 + 转寄退回 + 菜鸟仓入库  - 菜鸟仓转寄 + 线下补款
update result_sale_order02 set total_out_qudao=0 where total_out_qudao is null;
update result_sale_order02 set local_warehouse_costprice=0 where local_warehouse_costprice is null;
update result_sale_order02 set zhuanji_return_costprice=0 where zhuanji_return_costprice is null;
update result_sale_order02 set zhuanji_costprice=0 where zhuanji_costprice is null;

update result_sale_order02 set cainiao_costprice=0 where cainiao_costprice is null;
update result_sale_order02 set cainiao_zhuanji=0 where cainiao_zhuanji is null;
update result_sale_order02 set supplementary_amount=0 where supplementary_amount is null;
update result_sale_order02 set order_3150=0 where order_3150 is null;




update result_sale_order02 
set total_out= total_out_qudao + local_warehouse_costprice - zhuanji_costprice + zhuanji_return_costprice + cainiao_costprice - cainiao_zhuanji + supplementary_amount;

truncate table result_sale_order03;
insert into result_sale_order03 
select *,ROW_NUMBER() over (partition by source order by date_time asc)  sn from result_sale_order02 ;

update result_sale_order03 set max_refund_amount = 0 where max_refund_amount is null;

"
if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order02更新成功  result_sale_order03 计算成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order02更新失败  result_sale_order03 计算失败   ">> ${errologs}   

fi



##########################################################################################################################
#中文描述: 店铺支付宝数据聚合---result_sale_order04
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401
#############################################################################################################################
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "

update zfb_doudian_account set taobao_no=replace(taobao_no,'\'','');
update zfb_doudian_account set taobao_no=replace(taobao_no,'\`','');
update zfb_doudian_account set type='入账' where type='收入';
update zfb_doudian_account set type='出账' where type='支出';



/*
-- 天猫 实洋 天猫源系 账单插入
update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = REPLACE(商户订单号,'T200P','') where 业务基础订单号 = '' and 商户订单号 like 'T200P%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = substring(substring_index(备注,'交易单号：',-1),1,19) where 业务基础订单号 = '' and 备注 like '%交易单号：%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = substring(substring_index(备注,'订单编号：',-1),1,19) where 业务基础订单号 = '' and 备注 like '%订单编号：%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = substring(substring_index(备注,'tradeid:',-1),1,19) where 业务基础订单号 = '' and 备注 like '%tradeid:%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = substring(substring_index(备注,'tradeid:',-1),1,19) where 业务基础订单号 = '' and 备注 like '%tradeid:%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = substring(replace(备注,'天猫保证金履约险_追偿款_',''),1,19) where 业务基础订单号 = '' and 备注 like '%天猫保证金履约险_追偿款_%';

update zfb_tianmao_shiyang_yuanxi_account set 业务基础订单号 = '' where 业务基础订单号 like '%付款方：青岛%' ;

insert into zfb_2026_全量 select 支付宝交易号,支付宝流水号,商户订单号,业务基础订单号,商品名称,入账时间,对方账户,收入,-abs(支出) 支出,账户余额,支付渠道,账务类型,备注,业务账单来源,业务描述,业务订单号,业务基础订单号,店铺,入账时间 from zfb_tianmao_shiyang_yuanxi_account;

*/


truncate table zfb_2026_全量;
insert into  zfb_2026_全量 
select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s001天猫' 店铺,FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s1 where transaction_time >= 1767196800000; 

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s002冲鸭',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s2 where transaction_time >= 1767196800000; 

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s004鲨鱼',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s4 where transaction_time >= 1767196800000; 

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s005大猩猩',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s5 where transaction_time >= 1767196800000; 

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s006肥猫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s6 where transaction_time >= 1767196800000; 

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s007角马',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s7 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s008扬帆',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s8 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s009星琪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s9 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s010乐动',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s10 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s011芒芒',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s11 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s012尚品',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s12 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s013薇薇',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s13 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s014跃尚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s14 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s015烽行',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s15 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s016风跃',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s16 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s017火狐',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s17 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s024万源',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s24 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s025雷动',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s25 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s026百步',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s26 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s027破浪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s27 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s028萌神',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s28 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s029焱鑫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s29 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s030新兴',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s30 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s031五湖',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s31 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s032风街',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s32 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s033启扬',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s33 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s034海川',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s34 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s035gogo',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s35 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s036驰骋',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s36 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s037彼博',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s37 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s038卢克',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s38 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s039斑斓',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s39 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s040七号渡口',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s40 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s041狼爪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s41 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s042桃花岛',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s42 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s043大象',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s43 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s044star',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s44 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s048ace',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s48 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s049雅斯菲',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s49 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s050晟风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s50 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s051河马',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s51 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s052蓝鲸',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s52 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s053宏越',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s53 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s054飞渡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s54 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s055晴天',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s55 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s056深蓝',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s56 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s057龙卷风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s57 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s058OG',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s58 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s059淘淘',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s59 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s060飞鹰',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s60 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s061凯威',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s61 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s062火云鞋神',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s62 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s063佳悦奥莱',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s63 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s064江河',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s64 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s065世风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s65 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s066澜图',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s66 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s067维卡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s67 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s068雅诚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s68 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s069朔风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s69 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s070凌跃',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s70 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s071跃卓',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s71 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s072起点',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s72 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s073向尚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s73 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s074君采',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s74 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s075新世纪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s75 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s076影豹',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s76 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s077疾星',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s77 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s078国潮港湾',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s78 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s079城市屋顶',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s79 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s080古德',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s80 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s081驰耀',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s81 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s082白龙',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s82 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s083星褶',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s83 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s084摩登天空',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s84 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s087奇凡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s87 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s088达达',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s88 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s089比邻星',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s89 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s090零号',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s90 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s091孤客',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s91 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s092飞鱼',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s92 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s093独角兽',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s93 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s094鞋星人',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s94 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s095先驱',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s95 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s096梵语',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s96 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s097地平线',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s97 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s098神猫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s98 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s099欧乐',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s99 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s100硬汉',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s100 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s101yoyo',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s101 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s102曲奇',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s102 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s103鞋霸',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s103 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s104北极熊',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s104 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s105棱镜',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s105 where transaction_time >= 1767196800000; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s106冰锤',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s106 where transaction_time >= 1767196800000 ; 


insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s173源系',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s173 where transaction_time >= 1767196800000 ; 


insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s194实洋',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s194 where transaction_time >= 1767196800000 ; 



insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s379潮品知物',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s379 where transaction_time >= 1767196800000 ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s381达东',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_account_detailed_s381 where transaction_time >= 1767196800000 ; 



insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s001天猫' 店铺,FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s1 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s002冲鸭',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s2 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s004鲨鱼',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s4 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s005大猩猩',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s5 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s006肥猫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s6 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s007角马',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s7 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s008扬帆',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s8 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s009星琪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s9 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s010乐动',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s10 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s011芒芒',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s11 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s012尚品',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s12 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s013薇薇',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s13 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s014跃尚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s14 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s015烽行',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s15 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s016风跃',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s16 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s017火狐',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s17 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s024万源',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s24 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s025雷动',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s25 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s026百步',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s26 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s027破浪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s27 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s028萌神',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s28 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s029焱鑫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s29 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s030新兴',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s30 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s031五湖',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s31 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s032风街',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s32 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s033启扬',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s33 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s034海川',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s34 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s035gogo',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s35 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s036驰骋',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s36 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s037彼博',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s37 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s038卢克',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s38 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s039斑斓',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s39 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s040七号渡口',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s40 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s041狼爪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s41 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s042桃花岛',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s42 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s043大象',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s43 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s044star',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s44 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s048ace',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s48 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s049雅斯菲',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s49 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s050晟风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s50 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s051河马',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s51 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s052蓝鲸',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s52 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s053宏越',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s53 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s054飞渡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s54 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s055晴天',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s55 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s056深蓝',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s56 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s057龙卷风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s57 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s058OG',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s58 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s059淘淘',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s59 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s060飞鹰',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s60 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s061凯威',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s61 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s062火云鞋神',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s62 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s063佳悦奥莱',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s63 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s064江河',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s64 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s065世风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s65 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s066澜图',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s66 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s067维卡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s67 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s068雅诚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s68 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s069朔风',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s69 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s070凌跃',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s70 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s071跃卓',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s71 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s072起点',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s72 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s073向尚',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s73 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s074君采',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s74 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s075新世纪',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s75 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s076影豹',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s76 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s077疾星',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s77 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s078国潮港湾',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s78 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s079城市屋顶',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s79 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s080古德',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s80 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s081驰耀',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s81 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s082白龙',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s82 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s083星褶',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s83 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s084摩登天空',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s84 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s087奇凡',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s87 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s088达达',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s88 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s089比邻星',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s89 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s090零号',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s90 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s091孤客',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s91 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s092飞鱼',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s92 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s093独角兽',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s93 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s094鞋星人',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s94 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s095先驱',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s95 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s096梵语',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s96 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s097地平线',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s97 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s098神猫',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s98 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s099欧乐',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s99 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s100硬汉',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s100 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s101yoyo',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s101 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s102曲奇',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s102 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s103鞋霸',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s103 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s104北极熊',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s104 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s105棱镜',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s105 where transaction_time >= 1767196800000  ; 
insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s106冰锤',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s106 where transaction_time >= 1767196800000  ;

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s173源系',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s173 where transaction_time >= 1767196800000  ;


insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s194实洋',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s194 where transaction_time >= 1767196800000  ;



insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s379潮品知物',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s379 where transaction_time >= 1767196800000  ;

insert into zfb_2026_全量 select  finance_no,business_no,merchants_no,'' taobao_no,good_name,transaction_time,opposite_user,income_money,spending_money,account_balance,bill_source,transaction_type,transaction_remark,business_bill_source,business_describe,business_order_no,business_basics_no,'s381达东',FROM_UNIXTIME(floor(transaction_time/1000)) date_time from erp_db.ali_pay_remark_s381 where transaction_time >= 1767196800000  ;




update  zfb_2026_全量  set taobao_no = substring(substring_index(transaction_remark,'交易单号：',-1),1,19)  where  LENGTH(taobao_no) !=19 and opposite_user like '淘天物流科技有限公司%' and transaction_remark like '%交易单号：%';

update  zfb_2026_全量  set taobao_no = substring(substring_index(business_order_no,'M{0}|S{',-1),1,19)  where  LENGTH(taobao_no) !=19 and business_order_no like 'M{0}|S{%';

update  zfb_2026_全量  set taobao_no = substring(substring_index(transaction_remark,'订单编号：',-1),1,19)  where  LENGTH(taobao_no) !=19 and  transaction_remark like '%订单编号：%';


update  zfb_2026_全量  set taobao_no = substring(substring_index(transaction_remark,'淘宝极速回款-售中/后退垫资还款(',-1),1,19)  where  LENGTH(taobao_no) !=19 and  transaction_remark like '淘宝极速回款-售中/后退垫资还款(%';


update  zfb_2026_全量  set taobao_no = substring(substring_index(transaction_remark,'交易还款-极速回款-售中退款(',-1),1,19)  where  LENGTH(taobao_no) !=19 and  transaction_remark like '交易还款-极速回款-售中退款(%';


update  zfb_2026_全量  set taobao_no = substring(substring_index(transaction_remark,'保险承保-基础消保保证金险赔付追偿[',-1),1,19)  where  LENGTH(taobao_no) !=19 and  transaction_remark like '保险承保-基础消保保证金险赔付追偿[%';

update  zfb_2026_全量  set taobao_no = merchants_no where merchants_no like 'T200P%' and length(taobao_no) != 19;

update  zfb_2026_全量  set taobao_no = replace(taobao_no,'T200P','') where taobao_no like 'T200P%' and length(taobao_no) != 19;

update  zfb_2026_全量  set taobao_no = substring(taobao_no,1,19) where length(taobao_no) != 19;

update  zfb_2026_全量  set taobao_no = '' where taobao_no like 'RO%';
update  zfb_2026_全量  set taobao_no = '' where taobao_no like 'CNR%';

update zfb_2026_全量 
set taobao_no = replace(transaction_remark,'支付宝转账小额打款-关联订单号：','') 
where transaction_remark like '%支付宝转账小额打款-关联订单号：%';



delete from zfb_2024_全量 where date_time >= '2026-01-01 00:00:00';


insert into zfb_2024_全量 select * from zfb_2026_全量;


-- update zfb_2026_全量 set taobao_no = '2565695028238499756' where business_no = '20250530020070011560870059973964' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '2545407516723920366' where business_no = '20250601020070011520000034935857' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '4346832852654452008' where business_no = '20250602020070011550560033297898' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '2581972393288713797' where business_no = '20250603020070011540520072757038' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '4371488137987899143' where business_no = '20250609020070011550060011228555' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '2517829214037140364' where business_no = '20250604020070011550400017073623' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '4355915978890937220' where business_no = '20250529020070011550640046391652' and transaction_remark like '支付宝转账小额打款-未关联%';
-- update zfb_2026_全量 set taobao_no = '2601976440845549978' where business_no = '20250613020070011530890076908651' and transaction_remark like '支付宝转账小额打款-未关联%';

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " zfb_2024_全量 账单数据计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " zfb_2024_全量 账单数据计算 失败   ">> ${errologs}
    
fi






BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "

use big_data;


-- zfb_jingdong_商家资金 清洗
update zfb_jingdong_商家资金 set 交易订单号= replace(交易订单号,'\t','');
update zfb_jingdong_商家资金 set 交易类型= replace(交易类型,'\t','');
update zfb_jingdong_商家资金 set 交易订单号= replace(交易订单号,' ','');
update zfb_jingdong_商家资金 set 支出类型= replace(支出类型,'\t','');
update zfb_jingdong_商家资金 set 资金动账备注= replace(资金动账备注,'\t','');
update zfb_jingdong_商家资金 set 交易订单号= SUBSTRING(replace(资金动账备注,'订单号：',''),1,12) where 资金动账备注 like '%逆向保价%';

drop table if exists 京东商家资金;
create table 京东商家资金 as  
select 店铺     ,交易订单号,交易金额,0 spending_money,交易类型,入账时间,资金动账备注 from zfb_jingdong_商家资金 where 交易订单号 not like  '%--%' and 支出类型 like '%收入%' and  入账时间 like '2025%' union all 
select 店铺     ,交易订单号,0 income_money,-abs(交易金额) spending_money,交易类型,入账时间,资金动账备注 from zfb_jingdong_商家资金 where 交易订单号 not like  '%--%' and 支出类型 like  '%支出%' and  入账时间 like '2025%' union all 

select 店铺     ,交易订单号,交易金额,0 spending_money,交易类型,入账时间,资金动账备注 from zfb_jingdong_商家资金 where 交易订单号 not like  '%--%' and 支出类型 like '%收入%' and  入账时间 like '2026%' union all 
select 店铺     ,交易订单号,0 income_money,-abs(交易金额) spending_money,交易类型,入账时间,资金动账备注 from zfb_jingdong_商家资金 where 交易订单号 not like  '%--%' and 支出类型 like  '%支出%' and  入账时间 like '2026%'



;

update 京东商家资金 set 交易订单号= SUBSTRING(replace(资金动账备注,'订单号：',''),1,12) where 资金动账备注 like '%逆向保价%';
update 京东商家资金 set 交易订单号= SUBSTRING(replace(资金动账备注,'直赔退款代扣',''),1,12) where 资金动账备注 like '%直赔退款代扣%';


-- zfb_jingdong_商家资金 清洗





drop table if exists zfb_tianmao_taobao_account;
create table  zfb_tianmao_taobao_account as 


select 'tianmao_dewu' num,taobao_no,income_money,-abs(spending_money) spending_money,type transaction_type,'' transaction_remark from big_data.zfb_tianmao_dewu_account union all 
select '得物2' num,taobao_no,income_money,-abs(spending_money) spending_money,type transaction_type,'' transaction_remark from big_data.zfb_tianmao_dewu_account_得物2 union all 
select  店铺 num,订单号 taobao_no,0 income_money,-abs(变更金额) spending_money,变更原因 transaction_type,'' transaction_remark from big_data.得物保证金支出 where 变更类型  = '扣除' union all 

select 'pdd' num,order_number taobao_no,income income_money,disburse spending_money,remark transaction_type,\`describe\` transaction_remark from erp_db.pdd_store_account_detailed  where  create_time  >= 1735660800000  union all 
select 'pdd' num,商户订单号 taobao_no,收入金额 income_money,支出金额 spending_money,备注 transaction_type,业务描述 transaction_remark from big_data.拼多多达东遗漏账单表_不要删除  union all 




select 'jddj' num,taobao_no,income_money,spending_money,'' transaction_type,'' transaction_remark from big_data.zfb_jddj_account union all  

select 'jd' num,order_num taobao_no,amount income_money,0 spending_money,expense_item transaction_type,'' transaction_remark from erp_db.jingdong_store_account_detailed where amount >0   union all 
select 'jd' num,order_num taobao_no,0 income_money,amount spending_money,expense_item transaction_type,'' transaction_remark from erp_db.jingdong_store_account_detailed where amount <0   union all 

select 店铺,交易订单号,交易金额,spending_money,交易类型,交易类型 from 京东商家资金  where 入账时间 like '2025%' union all 

select 店铺,交易订单号,交易金额,spending_money,交易类型,交易类型 from 京东商家资金  where 入账时间 like '2026%' 


;



update  zfb_tianmao_taobao_account set taobao_no= substring(SUBSTRING_INDEX(transaction_type,'订单',-1),1,22) where num = 'pdd' and transaction_remark like '0040002%' and taobao_no = '';







drop table if exists 京东账单表;
create table 京东账单表 as  
select store num,order_num taobao_no,amount income_money,0 spending_money,expense_item transaction_type,expense_item,FROM_UNIXTIME(floor(compute_end_time/1000)) 结算时间 from erp_db.jingdong_store_account_detailed where amount >0  and YEAR(FROM_UNIXTIME(floor(compute_end_time/1000)))  = 2025 union all 
select store num,order_num taobao_no,0 income_money,amount spending_money,expense_item transaction_type,expense_item,FROM_UNIXTIME(floor(compute_end_time/1000))  结算时间 from erp_db.jingdong_store_account_detailed where amount <0 and YEAR(FROM_UNIXTIME(floor(compute_end_time/1000)))  = 2025 union all 

select 店铺,交易订单号,交易金额,spending_money,交易类型,交易类型,入账时间 from 京东商家资金  where 入账时间 like '2025%' union all 

select store num,order_num taobao_no,amount income_money,0 spending_money,expense_item transaction_type,expense_item,FROM_UNIXTIME(floor(compute_end_time/1000)) 结算时间 from erp_db.jingdong_store_account_detailed where amount >0  and YEAR(FROM_UNIXTIME(floor(compute_end_time/1000)))  = 2026 union all 
select store num,order_num taobao_no,0 income_money,amount spending_money,expense_item transaction_type,expense_item,FROM_UNIXTIME(floor(compute_end_time/1000))  结算时间 from erp_db.jingdong_store_account_detailed where amount <0 and YEAR(FROM_UNIXTIME(floor(compute_end_time/1000)))  = 2026 union all 

select 店铺,交易订单号,交易金额,spending_money,交易类型,交易类型,入账时间 from 京东商家资金  where 入账时间 like '2026%' 




;





"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " zfb_tianmao_taobao_account 账单数据计算  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " zfb_tianmao_taobao_account 账单数据计算 失败   ">> ${errologs}
    
fi




mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "select distinct * from 京东账单表;">/data/exchange/京东账单表.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D xiaoshoudianpu_zhangdan --local-infile -Bse "truncate table  京东账单表;load data local infile '/data/exchange/京东账单表.txt' into table 京东账单表 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 京东账单表 迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "京东账单表 迁移 失败   ">> ${errologs}
    
fi





BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

## 将抖音店铺ERP账单数据同步到10.0.180
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "
select * from erp_db.douyin_store_account_detailed where created_time >= 1767196800000 ;">/data/exchange/douyin_store_account_detailed.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "
delete from  douyin_store_account_detailed where created_time >= 1767196800000 ;

load data local infile '/data/exchange/douyin_store_account_detailed.txt' into table douyin_store_account_detailed character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " douyin_store_account_detailed 迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " douyin_store_account_detailed  迁移 失败   ">> ${errologs}
    
fi




## 将拼多多店铺ERP账单数据同步到10.0.180
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "
select * from erp_db.pdd_store_account_detailed where create_time > 1767196800000;">/data/exchange/pdd_store_account_detailed.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "
delete from   pdd_store_account_detailed where create_time > 1767196800000;

load data local infile '/data/exchange/pdd_store_account_detailed.txt' into table pdd_store_account_detailed character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

delete from pdd_store_account_detailed where created_time = 17;

insert into pdd_store_account_detailed 
select 
0 id,
'PDDDADONGYUNDONG' store,
商户订单号,
round(UNIX_TIMESTAMP(发生时间) * 1000,0) sj,
收入金额,
支出金额,
账务类型,
备注,
业务描述,
1 created_user_id,
17 created_time,
1 last_upd_uid,
17 last_upd_time  
from 拼多多达东遗漏账单表_不要删除;

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " pdd_store_account_detailed 迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " pdd_store_account_detailed  迁移 失败   ">> ${errologs}
    
fi







### 之同步最近一个月的   比如  2025-09-01  00：00：00

mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "select distinct * from zfb_2026_全量 where date_time >= '2026-01-01 00:00:00';">/data/exchange/zfb_2026_全量.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

use rugen;
delete from  zfb_2024_全量 where date_time >= '2026-01-01 00:00:00';
load data local infile '/data/exchange/zfb_2026_全量.txt' into table zfb_2024_全量 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 9月份 店铺支付宝 zfb_2024_全量 迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 9月份 店铺支付宝 zfb_2024_全量  迁移 失败   ">> ${errologs}
    
fi



mysql -hRDS_HOST -ubig_data -pPASS -D big_data -N -e "
select * from zfb_tianmao_taobao_account;">/data/exchange/zfb_tianmao_taobao_account.txt

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "truncate table  zfb_tianmao_taobao_account;load data local infile '/data/exchange/zfb_tianmao_taobao_account.txt' into table zfb_tianmao_taobao_account character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 店铺支付宝 账单数据 迁移  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 店铺支付宝 账单数据 迁移 失败   ">> ${errologs}
    
fi




## 通过爬虫获取微信账单数据
## 暂时先试用 后期需要日期优化

mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 全店商品 -c 淘宝支付流水 --fields "支付流水号,业务描述,入账时间,入账类型,备注,店铺名称,支出金额,收入金额,淘宝订单编号" --type=csv  --out /data/exchange/微信账单_爬虫.csv
sed -i 's/\\\"\"//g' /data/exchange/微信账单_爬虫.csv


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "


truncate table  zfb_微信支付订单_回款明细_爬虫版; load data local infile '/data/exchange/微信账单_爬虫.csv' into table zfb_微信支付订单_回款明细_爬虫版  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;


update zfb_微信支付订单_回款明细_爬虫版 set 支付流水号 = '' where 支付流水号 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 业务描述 = '' where 业务描述 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 入账类型 = '' where 入账类型 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 备注 = '' where 备注 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 店铺名称 = '' where 店铺名称 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 支出金额 = 0 where 支出金额 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 收入金额 = 0 where 收入金额 = 'NaN';
update zfb_微信支付订单_回款明细_爬虫版 set 淘宝订单编号 = '' where 淘宝订单编号 = 'NaN';


update zfb_微信支付订单_回款明细_爬虫版 set 支出金额 = 0 where 支出金额 = '';
update zfb_微信支付订单_回款明细_爬虫版 set 收入金额 = 0 where 收入金额 = '';


update zfb_微信支付订单_回款明细_爬虫版 set 店铺名称 = '天猫万源' where 店铺名称 = 'adidas万源专卖店';
update zfb_微信支付订单_回款明细_爬虫版 set 店铺名称 = '天猫公司' where 店铺名称 = '公司鞋类专营店';
update zfb_微信支付订单_回款明细_爬虫版 set 店铺名称 = '天猫跑步猫' where 店铺名称 = '跑步猫运动旗舰店';


truncate table zfb_微信支付订单_回款明细_爬虫版_etl;
insert into zfb_微信支付订单_回款明细_爬虫版_etl select distinct * from zfb_微信支付订单_回款明细_爬虫版;


update rugen.zfb_微信支付订单_回款明细_爬虫版_etl set 淘宝订单编号 = substring(substring_index(备注,'订单编号：',-1),1,19) where 淘宝订单编号 = '' and 入账类型 = '保证金扣款';




"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 爬虫微信账单数据获取  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 爬虫微信账单数据获取 失败   ">> ${errologs}
    
fi








mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 全店商品 -c 天猫淘宝小额打款 --fields "支付宝交易号,关联订单,打款金额(元),打款时间,打款类型,店铺名称,备注" --type=csv  --out /data/exchange/天猫淘宝小额打款.csv
sed -i 's/\\\"\"//g' /data/exchange/天猫淘宝小额打款.csv



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

truncate table  天猫淘宝小额打款; load data local infile '/data/exchange/天猫淘宝小额打款.csv' into table 天猫淘宝小额打款  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;

update 天猫淘宝小额打款 set 打款类型= 'QH补偿' where 备注 like '%缺货补偿%';


update 天猫淘宝小额打款 set 关联订单 = '2565695028238499756' where 支付宝交易号 = '20250530020070011560870059973964';
update 天猫淘宝小额打款 set 关联订单 = '2545407516723920366' where 支付宝交易号 = '20250601020070011520000034935857';
update 天猫淘宝小额打款 set 关联订单 = '4346832852654452008' where 支付宝交易号 = '20250602020070011550560033297898';
update 天猫淘宝小额打款 set 关联订单 = '2581972393288713797' where 支付宝交易号 = '20250603020070011540520072757038';
update 天猫淘宝小额打款 set 关联订单 = '4371488137987899143' where 支付宝交易号 = '20250609020070011550060011228555';
update 天猫淘宝小额打款 set 关联订单 = '2517829214037140364' where 支付宝交易号 = '20250604020070011550400017073623';
update 天猫淘宝小额打款 set 关联订单 = '4355915978890937220' where 支付宝交易号 = '20250529020070011550640046391652';



update 天猫淘宝小额打款 a,ERP_赔付指标体系_综合 b 
set a.店铺名称 =b.sales_channels 
where a.关联订单=b.source;
"





if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 小额打款数据获取  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 小额打款数据获取 失败   ">> ${errologs}

fi







## 通过爬虫获取账单保证金赔付数据
## 暂时先试用 后期需要日期优化

mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 全店商品 -c 淘宝保证金流水 --fields "订单编号,业务描述,业务编号,出资类型,原因,去向账户,完成时间,币种,店铺名称,操作类型,收支金额,来源账户,现金总余额" --type=csv  --out /data/exchange/保证金账单_爬虫.csv
sed -i 's/\\\"\"//g' /data/exchange/保证金账单_爬虫.csv



mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

truncate table  保证金账单_爬虫; load data local infile '/data/exchange/保证金账单_爬虫.csv' into table 保证金账单_爬虫  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;

update 保证金账单_爬虫 set 订单编号  = replace(replace(订单编号,'\t',''),'\"','');


/*
drop table if exists 保证金账单_爬虫01;
create table 保证金账单_爬虫01 as select distinct * from 保证金账单_爬虫;
truncate table 保证金账单_爬虫;
insert into 保证金账单_爬虫 select * from 保证金账单_爬虫01;
*/

-- select 完成时间,操作类型,原因,币种,收支金额,现金总余额,来源账户,去向账户,出资类型,业务描述,业务编号,订单编号,店铺 from 保证金明细_淘宝 union all 
-- select 完成时间,操作类型,原因,币种,收支金额,现金总余额,来源账户,去向账户,出资类型,业务描述,业务编号,订单编号,店铺 from 保证金明细_天猫 union all 

drop table if exists 保证金明细_综合表;
create table   保证金明细_综合表 as 
select distinct 完成时间,操作类型,原因,币种,收支金额,现金总余额,来源账户,去向账户,出资类型,业务描述,业务编号,订单编号,店铺 from 保证金账单_爬虫;

"



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`


mongoexport -h MONGO_HOST  -uUSER -pPASS --authenticationDatabase admin -d 全店商品 -c 抖店_保费支出_聚合账户 --fields "保险单号,关联子订单号,动账时间,动账流水号,店铺昵称,摘要描述,金额(元)" --type=csv  --out /data/exchange/抖店_保费支出_聚合账户.csv
sed -i 's/\\\"\"//g' /data/exchange/抖店_保费支出_聚合账户.csv




mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen --local-infile -Bse "

truncate table  抖店_保费支出_聚合账户; load data local infile '/data/exchange/抖店_保费支出_聚合账户.csv' into table 抖店_保费支出_聚合账户  character set utf8mb4 fields terminated by ',' lines terminated by '\n' IGNORE 1 LINES;

-- delete from 抖店_保费支出_聚合账户 where 金额 = 0;

update  抖店_保费支出_聚合账户 set 关联子订单号 = replace(关联子订单号,'\`','') where 关联子订单号 like '%\`%';
"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 抖店_保费支出_聚合账户  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 抖店_保费支出_聚合账户 失败   ">> ${errologs}

fi


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

update  douyin_store_account_detailed set shop_order_id = order_id where shop_order_id = '' and order_id !='';

-- delete from douyin_store_account_detailed where account_bill_desc = '提现';
-- delete from douyin_store_account_detailed where account_bill_desc like  '保费扣除%';



insert into zfb_tianmao_taobao_account 
select 'doudian' num,shop_order_id taobao_no,account_amount income_money,0 spending_money,account_bill_desc transaction_type,account_bill_desc transaction_remark from douyin_store_account_detailed where fund_flow_desc='入账' union all 
select 'doudian' num,shop_order_id taobao_no,0 income_money,-abs(account_amount) spending_money,account_bill_desc transaction_type,account_bill_desc transaction_remark from douyin_store_account_detailed where fund_flow_desc='出账' union all 

select '抖音运费险' num,关联子订单号 taobao_no,0 income_money,-abs(金额) spending_money,'' transaction_type,'' transaction_remark from 抖店_保费支出_聚合账户;


-- update zfb_抖店运费险支出明细250101_250327 set  订单编号 = REPLACE(订单编号,'\`','') where 订单编号 like '%\`%';
-- zfb_抖店运费险支出明细250101_250327 人工下载保费支出暂不使用
-- select '抖音运费险' num,订单编号 taobao_no,0 income_money,-abs(支付保费) spending_money,'' transaction_type,'' transaction_remark from zfb_抖店运费险支出明细250101_250327;




update  zfb_2024_全量  set taobao_no = substring(taobao_no,1,19);

/*
##############
##############
##############一次更新
##############
*/


drop table if exists zfb_2024_全量_01;
create table zfb_2024_全量_01 as 
select date_time,merchants_no,taobao_no,income_money,transaction_remark,店铺  from (
select * from zfb_2024_全量 where business_describe like '008002800006|保证金-淘宝-缴存%' union all 
select * from zfb_2024_全量 where business_describe like '008002800010|保证金-淘宝-扣除转移%' union all  
select * from zfb_2024_全量 where transaction_remark like '商家权益红包-预算追加%') x where x.income_money !=0 order by x.date_time ;

insert into zfb_2024_全量_01 
select date_time,merchants_no,taobao_no,abs(spending_money),transaction_remark,店铺  from (
select * from zfb_2024_全量 where business_describe like '008002800006|保证金-淘宝-缴存%' union all 
select * from zfb_2024_全量 where business_describe like '008002800010|保证金-淘宝-扣除转移%' union all  
select * from zfb_2024_全量 where transaction_remark like '商家权益红包-预算追加%') x where x.spending_money !=0 order by x.date_time ;

update zfb_2024_全量_01 set taobao_no=0 where taobao_no = '';

update zfb_2024_全量_01 set taobao_no=0 where length(taobao_no) != 19;

truncate table  zfb_2024_全量_update;
insert into  zfb_2024_全量_update 
select * from (
select *,ROW_NUMBER() over (partition by substring(date_time,1,16),income_money,店铺 ) sn,max(taobao_no) over (partition by substring(date_time,1,16),income_money,店铺 ) rn from zfb_2024_全量_01 ) x 
where x.rn !=0 and x.taobao_no =0;


update zfb_2024_全量 a,
zfb_2024_全量_update b 
set a.taobao_no = b.rn 
where a.date_time=b.date_time and a.merchants_no=b.merchants_no and LENGTH(a.taobao_no) != 19;




 
-- 天猫  本地退赔付 数据清洗    售后单退款单号  对应 

-- 本地退赔付数据 原来是使用result_sale_order01 来判断  因为节约计算资源所以 result_sale_order 少数据  
-- 需要最完善的  2025年全年的so单号 和source单号  所以暂时使用 ERP_赔付指标体系_综合  表 


update zfb_2024_全量 set transaction_remark=replace(transaction_remark,'\t','') where transaction_remark like '本地退赔款%' and transaction_remark like '%\t%';


update zfb_2024_全量 set transaction_remark=replace(transaction_remark,'\t','') where transaction_remark like '本地退赔款%' and transaction_remark like '%\t%';
update zfb_2024_全量 set transaction_remark=replace(transaction_remark,'	','') where transaction_remark like '本地退赔款%' and transaction_remark like '%	%';

update zfb_2024_全量 set transaction_remark=replace(transaction_remark,'保险理赔-本地退赔款','本地退赔款') where transaction_remark like '保险理赔-本地退赔款%' ;
update zfb_2024_全量 set transaction_remark=substring_index(replace(transaction_remark,'太保产险苏州本地退赔款，退款编号','本地退赔款'),'  ESU',1)
where transaction_remark like '太保产险苏州本地退赔款，退款编号%' ;

update zfb_2024_全量 set transaction_remark=substring_index(replace(transaction_remark,'太保产险苏州本地退赔款','本地退赔款'),'  ESU',1)
where transaction_remark like '太保产险苏州本地退赔款%' ;




truncate table  本地退赔付数据;
insert into 本地退赔付数据  

select distinct 
a.refund_id,b.source 
from 
(select replace(replace(transaction_remark,'本地退赔款',''),'\t','') transaction_remark from zfb_2024_全量 where transaction_remark like '本地退赔款%') x  
inner join  (select distinct refund_id,name from result_sale_order_after_sales_tianmao_taobao) a on x.transaction_remark = a.refund_id 
left join ERP_赔付指标体系_综合  b 
on a.name=b.order_no;


delete from 本地退赔付数据 where source is null;

update zfb_2024_全量 set transaction_remark=replace(transaction_remark,'\t','');






update 
zfb_2024_全量 a ,
本地退赔付数据 b 

set a.taobao_no = b.source 

where a.transaction_remark like '本地退赔款%' and replace(a.transaction_remark,'本地退赔款','')=b.refund_id;



/*
##############
##############
##############二次更新
##############
*/


drop table if exists zfb_2024_全量_02;
create table zfb_2024_全量_02 as 
select date_time,date_time date_time_update,merchants_no,taobao_no,income_money,transaction_remark,店铺  from (
select * from zfb_2024_全量 where business_describe like '008002800006|保证金-淘宝-缴存%' union all 
select * from zfb_2024_全量 where business_describe like '008002800010|保证金-淘宝-扣除转移%' union all  
select * from zfb_2024_全量 where transaction_remark like '商家权益红包-预算追加%') x where x.income_money !=0 order by x.date_time ;

insert into zfb_2024_全量_02 
select date_time,date_time date_time_update,merchants_no,taobao_no,abs(spending_money),transaction_remark,店铺  from (
select * from zfb_2024_全量 where business_describe like '008002800006|保证金-淘宝-缴存%' union all 
select * from zfb_2024_全量 where business_describe like '008002800010|保证金-淘宝-扣除转移%' union all  
select * from zfb_2024_全量 where transaction_remark like '商家权益红包-预算追加%') x where x.spending_money !=0 order by x.date_time ;

update zfb_2024_全量_02 set taobao_no=0 where taobao_no = '';

update zfb_2024_全量_02 set taobao_no=0 where length(taobao_no) != 19;

delete from zfb_2024_全量_02 where transaction_remark  like '商家权益红包-预算追加%' and LENGTH(taobao_no)=19;
delete from zfb_2024_全量_02 where transaction_remark  like '淘宝消费者保证金-交易赔付%' and LENGTH(taobao_no)=19;
update zfb_2024_全量_02 set date_time_update = DATE_SUB(date_time, INTERVAL 1 MINUTE) where transaction_remark  like '商家权益红包-预算追加%' and (substring(date_time,18,2)='00' or substring(date_time,18,2)='01');




truncate table  zfb_2024_全量_update02;
insert into  zfb_2024_全量_update02  
select * from (
select *,ROW_NUMBER() over (partition by substring(date_time_update,1,16),income_money,店铺 ) sn,max(taobao_no) over (partition by substring(date_time_update,1,16),income_money,店铺 ) rn from zfb_2024_全量_02 ) x 
where x.rn !=0 and x.taobao_no =0;


update zfb_2024_全量 a,
zfb_2024_全量_update02 b 
set a.taobao_no = b.rn 
where a.date_time=b.date_time and a.merchants_no=b.merchants_no and LENGTH(a.taobao_no) != 19;




update 保证金明细_综合表 set 订单编号 = replace(订单编号,'\t','');
-- 通过保证金明细表  匹配正确基础订单号

update zfb_2024_全量 a ,
(select distinct 完成时间 sj,订单编号 source,店铺,abs(收支金额) 金额 from 保证金明细_综合表 where 完成时间 > '2024-11-01 00:00:00' and 订单编号 is not null) b 

set a.taobao_no = b.source 

where abs(TIMESTAMPDIFF(SECOND,a.date_time,b.sj)) < 60 and a.店铺=b.店铺 and a.spending_money=-b.金额 and a.business_describe like '008002800006|保证金-淘宝-缴存%' and LENGTH(a.taobao_no) !=19;


update zfb_2024_全量 set taobao_no = replace(taobao_no,'\t','');



update zfb_2024_全量 set taobao_no=substring(replace(transaction_remark,'代扣款（扣款用途：淘宝联盟佣金代扣 tradeid:',''),1,19) where transaction_remark like '代扣款（扣款用途：淘宝联盟佣金代扣 tradeid:%';


update zfb_2024_全量 set taobao_no=substring(replace(transaction_remark,'保险理赔-天猫海外退货险理赔款-订单号［',''),1,19) where transaction_remark like '保险理赔-天猫海外退货险理赔款-订单号［%';



-- 增加速卖通运费成本支出
/*
update 速卖通运费支付宝支出表 set 收入 =0 where 收入 is null;
update 速卖通运费支付宝支出表 set 支出 =0 where 支出 is null;

drop table if exists smt_运费;
create table smt_运费 as 
select 
名称,收入,支出,
substring_index(concat('LP',substring_index(replace(名称,'_AE-物流配送',''),'LP',-1)),'_',1) lp编号,
substring_index(concat('LP',substring_index(replace(名称,'_AE-物流配送',''),'LP',-1)),'_',-1) source 
from  速卖通运费支付宝支出表;

update smt_运费 a ,(select lp编号,source from smt_运费 where source not like 'LP%') b 
set a.source = b.source 
where a.source=b.lp编号;

update smt_运费 set 收入 =0 where 收入 is null;
update smt_运费 set 支出 =0 where 支出 is null;

-- 速卖通 麦欣下载的运费表 用的元麦的支付宝  这个数据和    smt国际支付宝税费 重合   暂时注释  smt运费




-- 增加速卖通账单数据

update smt_国际支付宝账单明细 set 商家订单号 =substring(商家订单号,1,16) where 商家订单号 not like 'CN_%';

drop table if exists smt_支付宝账单;
create table smt_支付宝账单 as 
select 
店铺,时间,金额,类型,商家订单号,
if(备注 like '%LP%',substring_index(concat('LP',substring_index(replace(备注,'_AE-物流配送',''),'LP',-1)),'_',1),商家订单号) lp编号,
if(备注 like '%LP%',substring_index(concat('LP',substring_index(replace(备注,'_AE-物流配送',''),'LP',-1)),'_',-1),商家订单号) source 
from  smt_国际支付宝账单明细;

update smt_支付宝账单 set source = '' where source like '%LP%';
update smt_支付宝账单 set source = substring(source,1,16) ;
update smt_支付宝账单 set lp编号 = substring(lp编号,1,16) where  lp编号 like  '%LP%';


update smt_支付宝账单 a ,(select distinct lp编号,source from smt_支付宝账单 where source !='' and source not like 'AE%' and lp编号 like 'LP%') b 
set a.source = b.source 
where a.lp编号=b.lp编号 and a.source = '';


update smt_支付宝账单 a ,(select lp编号,source from smt_运费 where source not like 'LP%') b 
set a.source = b.source 
where a.lp编号=b.lp编号 and a.source = '';
update smt_支付宝账单 set 金额 =0 where 金额 is null;


delete from smt_支付宝账单 where source = '';
*/




update smt_国际支付宝账单明细 set  商家订单号  = substring(商家订单号,1,16) ;



update smt_俄罗斯支付宝账单明细 set source = substring(SUBSTRING_INDEX(备注,'loan',-1),1,16) where 备注 like '%:loan%';
update smt_俄罗斯支付宝账单明细 set source = substring(SUBSTRING_INDEX(备注,'refund',-1),1,16) where 备注 like '%:refund%' and source is null ;
update smt_俄罗斯支付宝账单明细 set source = substring(SUBSTRING_INDEX(备注,'明细:违背发货承诺订单扣罚;业务订单号:',-1),1,16) where  备注 like '%明细:违背发货承诺订单扣罚;业务订单号:%' and source is null ;









truncate  table  速卖通元麦个人支付宝明细优化表;

INSERT INTO 速卖通元麦个人支付宝明细优化表 

select * from 速卖通元麦个人支付宝明细 where source not IN (

select source号 from  (
select distinct source source号 from  速卖通元麦个人支付宝明细 
UNION ALL
select distinct 交易单号 source号 from smt_国际支付宝税费表) a GROUP BY source号 HAVING COUNT(*) >1 );



delete from zfb_2024_全量 where transaction_remark = '淘宝消费者保证金-解冻-null';
delete from zfb_2024_全量 where transaction_remark = '淘宝消费者保证金-充值（代扣）';
delete from zfb_2024_全量 where transaction_remark = '淘宝消费者保证金-充值（代扣）-红包冻结';
delete from zfb_2024_全量 where transaction_remark = '淘宝消费者保证金-交易售后';

drop table if exists zfb_2024_全量_清洗_F_C;
create table  zfb_2024_全量_清洗_F_C as select * from zfb_2024_全量 where date_time >= '2026-01-01 00:00:00';



truncate table zfb_tianmao_taobao_account01;
insert into zfb_tianmao_taobao_account01 

select 
x.taobao_no,
sum(x.income_money) income,
sum(x.spending_money) pay 


from (
select taobao_no,income_money,spending_money from zfb_tianmao_taobao_account union all 
select taobao_no,income_money,spending_money from zfb_2024_全量_清洗_F_C union all 
-- select 淘宝订单编号 taobao_no,收入金额 income_money,-abs(支出金额) spending_money from zfb_微信支付订单_回款明细 union all

select 淘宝订单编号 taobao_no,收入金额 income_money,-abs(支出金额) spending_money from zfb_微信支付订单_回款明细_爬虫版_etl  where 入账时间 >= '2026-01-01 00:00:00' union all 
 

select 商家订单号 taobao_no,金额 income_money,0 spending_money from smt_国际支付宝账单明细 where 金额 > 0 and 商家订单号 not like 'CN%' union all 
select 商家订单号 taobao_no,0 income_money,金额 spending_money from smt_国际支付宝账单明细 where 金额 < 0 and 商家订单号 not like 'CN%' union all 


select source taobao_no,金额 income_money,0 spending_money from smt_俄罗斯支付宝账单明细 where 金额 > 0  union all 
select source taobao_no,0 income_money,金额 spending_money from smt_俄罗斯支付宝账单明细 where 金额 < 0  union all 



select  交易单号 taobao_no,abs(支付金额) income_money,0 spending_money from smt_国际支付宝税费表 where 支付金额 < 0 union all 
select  交易单号 taobao_no,0 income_money,-abs(支付金额) spending_money from smt_国际支付宝税费表 where 支付金额 > 0 union all 
select source taobao_no,0 income_money,-abs(金额) spending_money from 速卖通元麦个人支付宝明细优化表 where source is not null union all 
select 订单号 taobao_no,0 income_money,-abs(总邮资) spending_money from smt运费EMS费用综合表_麦欣钉钉申请 where 订单号 is not null union all 



-- select source taobao_no,0 income_money,支出 spending_money from 速卖通运费支付宝支出表 where source is not null union all 
-- select source taobao_no,0 income_money,-abs(金额) spending_money from 速卖通元麦个人支付宝明细 where source is not null union all 



select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_入账明细_欧洲 where 金额 <0 union all 
select 备货单号 taobao_no,金额 income_money,0 spending_money from temu_新_入账明细_欧洲 where 金额 > 0  union all 
select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_入账明细_全球 where 金额 <0 union all 
select 备货单号 taobao_no,金额 income_money,0 spending_money from temu_新_入账明细_全球 where 金额 > 0  union all 
select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_入账明细_美国 where 金额 <0 union all 
select 备货单号 taobao_no,金额 income_money,0 spending_money from temu_新_入账明细_美国 where 金额 > 0  union all 


select  备货单 taobao_no,0 income_money,-ABS(违规金额) spending_money from temu_新_赔付明细  union all 
select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_消费者及履约保障_售后问题_欧洲 union all  
select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_消费者及履约保障_售后问题_全球 union all 
select 备货单号 taobao_no,0 income_money,-abs(金额) spending_money from temu_新_消费者及履约保障_售后问题_美国 union all 

select wb单号 taobao_no,赔付金额 income_money,0 spending_money from temu_新_消费者及履约保障_售后补寄  union all 


select 订单号 taobao_no,收入金额 income_money,-abs(支出金额) spending_money from 快手_店铺订单流水 





) x 
group by x.taobao_no 
;








delete  from zfb_tianmao_taobao_account01 where taobao_no = '';
delete  from zfb_tianmao_taobao_account01 where taobao_no is null;






"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " zfb_tianmao_taobao_account01  成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " zfb_tianmao_taobao_account01 失败   ">> ${errologs}
    
fi









BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

SET GLOBAL group_concat_max_len=102400;
SET group_concat_max_len=102400;



truncate table result_sale_order03_bak;
insert into  result_sale_order03_bak  
select a.*,
b.dingkuan_fee dingkuan_fee,
c.queshou_fee  queshou_fee
from result_sale_order03 a 
left join (select so单号 so_order_id,sum(费用合计) dingkuan_fee from 鼎宽快递明细表 group by so单号) b on a.order_no = b.so_order_id 
left join (select 订单号,round(sum(金额),2) queshou_fee from 雀手快递中转明细表 group by 订单号) c on a.source = c.订单号 ;


update result_sale_order03_bak set dingkuan_fee = 0 where dingkuan_fee is null;
update result_sale_order03_bak set queshou_fee = 0 where queshou_fee is null;

-- 麦果得物   12.22 采购一批次84件货品 直接发给 得物仓  这种特殊情况需要 so单号一一对应  
-- 核查后临时用update 处理  入库质检通过79 单  另外四单入库本地仓

update result_sale_order03_bak set total_out_qudao = -501 where order_no in (
'SO009898546',
'SO009895304',
'SO009894249',
'SO009892735',
'SO009888499',
'SO009884948',
'SO009872425',
'SO009869038',
'SO009868936',
'SO009868259',
'SO009867202',
'SO009862051',
'SO009861025',
'SO009855508',
'SO009847995',
'SO009848083',
'SO009848118',
'SO009848120',
'SO009848131',
'SO009848151',
'SO009848158',
'SO009848162',
'SO009848167',
'SO009848169',
'SO009848177',
'SO009848194',
'SO009848196',
'SO009848202',
'SO009848218',
'SO009848222',
'SO009848227',
'SO009848232',
'SO009848240',
'SO009848247',
'SO009848252',
'SO009848250',
'SO009848261',
'SO009848266',
'SO009848273',
'SO009848277',
'SO009848293',
'SO009848300',
'SO009848301',
'SO009848305',
'SO009848308',
'SO009848312',
'SO009848316',
'SO009848320',
'SO009848322',
'SO009848328',
'SO009848334',
'SO009848337',
'SO009848339',
'SO009848349',
'SO009848469',
'SO009848467',
'SO009848463',
'SO009848461',
'SO009848458',
'SO009848457',
'SO009848454',
'SO009848451',
'SO009848445',
'SO009848437',
'SO009848435',
'SO009848433',
'SO009848431',
'SO009848427',
'SO009848421',
'SO009848412',
'SO009848406',
'SO009848404',
'SO009848403',
'SO009848398',
'SO009848394',
'SO009848387',
'SO009848381',
'SO009848372',
'SO009848367'
);



"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order03_bak  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order03_bak  聚合失败   ">> ${errologs}
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;


truncate table  result_sale_order04_test;
insert into result_sale_order04_test 
select 
sum(total_out_qudao) qudao_chengben,
sum(supplementary_amount) xianxiabukuan,
sum(local_warehouse_costprice) ruku,
sum(zhuanji_costprice) zhuanji,
sum(zhuanji_return_costprice) zhuanji_return,
sum(cainiao_costprice) cainiao_ruku,
sum(cainiao_zhuanji) cainiao_zhuanji_costprice,
sum(order_3150) order_3150_chengben,
sum(piliangtui) piliangtui_value,
sum(dingkuan_fee) dingkuan_value,
sum(queshou_fee) queshou_value,

source from result_sale_order03_bak group by source;




truncate table  result_sale_order04_赤兔;
insert into result_sale_order04_赤兔 
select source,sum(chitu_pay) chitu_pay from  赤兔表 group by source;



truncate table  result_sale_order04;
insert into result_sale_order04 

select 
b.income,
b.pay,
d.chitu_pay,
c.qudao_chengben,c.xianxiabukuan,c.ruku,c.zhuanji,c.zhuanji_return,c.cainiao_ruku,c.cainiao_zhuanji_costprice,c.order_3150_chengben,c.piliangtui_value,c.dingkuan_value,c.queshou_value,
a.* 
from result_sale_order03_bak a 
left join zfb_tianmao_taobao_account01 b on a.source=b.taobao_no 
left join result_sale_order04_test c on a.source=c.source 
left join result_sale_order04_赤兔 d on a.source=d.source
;





update result_sale_order04 set income =0 where income is null;
update result_sale_order04 set income =0 where sn != 1;

update result_sale_order04 set pay =0 where pay is null;
update result_sale_order04 set pay =0 where sn != 1;

update result_sale_order04 set piliangtui_value =0 where sn != 1;
update result_sale_order04 set dingkuan_value =0 where sn != 1;
update result_sale_order04 set queshou_value =0 where sn != 1;
update result_sale_order04 set qudao_chengben =0 where sn != 1;
update result_sale_order04 set xianxiabukuan =0 where  sn != 1;
update result_sale_order04 set ruku =0 where  sn != 1;
update result_sale_order04 set zhuanji =0 where  sn != 1;
update result_sale_order04 set zhuanji_return =0 where sn != 1;
update result_sale_order04 set cainiao_ruku =0 where  sn != 1;
update result_sale_order04 set cainiao_zhuanji_costprice =0 where  sn != 1;
update result_sale_order04 set order_3150_chengben =0 where  sn != 1;

ALTER TABLE result_sale_order04 MODIFY laolu_num VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

update result_sale_order04 set aomei_num=ps_gx_order_no where pickingsource = 'AOMEI';






-- 公司共鞋出货订单 账单收款到3150  在线表格有截图

update result_sale_order04 a,公司共鞋出货订单 b 
 set a.income = b.销售出货价 
 where a.order_no=b.SO单号 and b.是否结账 is not null and b.是否结账 = '是';




"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order04  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order04  聚合失败   ">> ${errologs}
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

ALTER TABLE result_sale_order04 MODIFY local_warehouse_category VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE result_sale_order04 MODIFY zhuanji_category VARCHAR(100) CHARACTER SET utf8mb4 COLLATE  utf8mb4_general_ci;


SET GLOBAL group_concat_max_len=10240000;
SET group_concat_max_len=10240000;

truncate table result_sale_order04_middle;
insert into result_sale_order04_middle select '共鞋' pt,order_no,gongxie value,gogxietiaohuo_num num from result_sale_order04;
insert into result_sale_order04_middle select '西街' pt,order_no,xijie value,xijie_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '速冠' pt,order_no,suguan value,suguan_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '味亦' pt,order_no,weiyi value,weiyi_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '激想' pt,order_no,jixiang value,jixiang_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '杰之宁' pt,order_no,jiezhining value,jiezhining_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '格林岛' pt,order_no,gelindao value,gelindao_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '立臻' pt,order_no,lizhen value,lizhen_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '亿网' pt,order_no,yiwang value,yiwang_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '成目商' pt,order_no,chengmushang value,chengmushang_num num from result_sale_order04 ;
insert into result_sale_order04_middle select tiaohuo_num pt,order_no,tiaohuo value,order_no num from result_sale_order04 where tiaohuo is not null and tiaohuo_num is not null;;
insert into result_sale_order04_middle select '中盛' pt,order_no,zhongsheng value,zhongsheng_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '同业' pt,order_no,tongye value,tongye_num num from result_sale_order04 ; 
insert into result_sale_order04_middle select '老卢' pt,order_no,laolu value,laolu_num num from result_sale_order04 ; 
insert into result_sale_order04_middle select '澳美' pt,order_no,aomei value,aomei_num num from result_sale_order04 ; 
insert into result_sale_order04_middle select '流苏' pt,order_no,liusu value,liusu_num num from result_sale_order04 ; 
insert into result_sale_order04_middle select '菜鸟' pt,order_no,cainiao value,'111' num from result_sale_order04 ; 
insert into result_sale_order04_middle select '宝原' pt,order_no,baoyuan value,baoyuan_num  num from result_sale_order04 ; 
insert into result_sale_order04_middle select '百宏' pt,order_no,baihong value,baihong_num  num from result_sale_order04 ; 
insert into result_sale_order04_middle select '宝福来' pt,order_no,baofulai,baofulai_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '宝胜' pt,order_no,baosheng,baosheng_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '畅跑' pt,order_no,changpao,changpao_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '大树' pt,order_no,dashu,dashu_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '登腾' pt,order_no,dengteng,dengteng_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '凡兮' pt,order_no,fanxi,fanxi_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '法雅' pt,order_no,faya,faya_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '非凡' pt,order_no,feifan,feifan_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '国域' pt,order_no,guoyu,guoyu_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '互动' pt,order_no,hudong,hudong_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '杰斯拓' pt,order_no,jiesituo,jiesituo_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '杰之行' pt,order_no,jiezhixing,jiezhixing_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '劲浪' pt,order_no,jinlang,jinlang_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '聚美优特' pt,order_no,juyoumeite,juyoumeite_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '酷锐' pt,order_no,kurui,kurui_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '联合尚品' pt,order_no,lianheshangpin,lianheshangpin_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '迈盛悦和' pt,order_no,maishengyuehe,maishengyuehe_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '瑞动' pt,order_no,ruidong,ruidong_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '尚动' pt,order_no,shangdong,shangdong_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '天马' pt,order_no,tianma,tianma_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '文石' pt,order_no,wenshi,wenshi_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '五哲' pt,order_no,wuzhe,wuzhe_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '易商' pt,order_no,yishang,yishang_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '一尧' pt,order_no,yiyao,yiyao_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '育泰' pt,order_no,yutai,yutai_num from result_sale_order04 ; 
insert into result_sale_order04_middle select '本地仓' pt,order_no,local_warehouse,local_warehouse_num from result_sale_order04;
insert into result_sale_order04_middle select '乔乐' pt,order_no,qiaole value,qiaole_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '第五季' pt,order_no,diwuji value,diwuji_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '名鞋库' pt,order_no,mingxieku value,mingxieku_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '比恩' pt,order_no,bien value,bien_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '耐创' pt,order_no,naichuang value,naichuang_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '斌狗机舱' pt,order_no,bingoujicang value,bingoujicang_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '突破运动' pt,order_no,tupoyundong value,tupoyundong_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '海玲轩' pt,order_no,hailingxuan value,hailingxuan_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '胡嘉兴' pt,order_no,hujiaxing value,hujiaxing_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '孤帆逐日' pt,order_no,gufanzhuri value,gufanzhuri_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '清锐' pt,order_no,qingrui value,qingrui_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '雨歌' pt,order_no,yuge value,yuge_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '黑石' pt,order_no,heishi value,heishi_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '领衔' pt,order_no,lingxian value,lingxian_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '橙子运动' pt,order_no,chengziyundong value,chengziyundong_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '苇玖城' pt,order_no,weijiucheng value,weijiucheng_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '全勇' pt,order_no,quanyong value,quanyong_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '威海斯博兹' pt,order_no,weihaisibozi value,weihaisibozi_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '趣淘' pt,order_no,qutao value,qutao_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '凌齿龙' pt,order_no,lingchilong value,lingchilong_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '德力聚川' pt,order_no,delijuchuan value,delijuchuan_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '兴悦奥特莱斯' pt,order_no,xingyueaotelaisi value,xingyueaotelaisi_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '共鞋公司调货' pt,order_no,gongxie_myjochuku value,gongxie_myjochuku_num num from result_sale_order04 ;
insert into result_sale_order04_middle select '激想团购' pt,order_no,jixiangtuangou value,jixiangtuangou_num num from result_sale_order04 ;





delete from result_sale_order04_middle where value is null;
delete from result_sale_order04_middle where value = 0;
"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order04_middle  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order04_middle  聚合失败   ">> ${errologs}
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

drop table if exists result_sale_order04_middle01;
create table result_sale_order04_middle01 as 
select order_no,concat(pt,'||',value,'||',num) value from result_sale_order04_middle;
"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order04_middle01  聚合成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order04_middle01  聚合失败   ">> ${errologs}
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;
truncate table result_sale_order05;
insert into result_sale_order05  
select 
a.*,
b.Deduction_details 
from result_sale_order04 a 

left join 

(select order_no,group_concat(value separator'&&') Deduction_details from result_sale_order04_middle01 group by order_no) b 

on a.order_no=b.order_no 
order by a.date_time desc ;


update result_sale_order05 set refund_express_number= 'N' where refund_express_number is null;
update result_sale_order05 set zhuanji_time=local_warehouse_ruku_time where zhuanji_time is null and zhuanji_pickingExpressNumber !='';
update result_sale_order05 set chitu_pay=0 where chitu_pay is null;
"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order05  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order05  聚合失败   ">> ${errologs}
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;
truncate table result_sale_order06;
insert into result_sale_order06 
select *,
case when income=0 and pay=0 then 'N' 
else 
concat(source,':支付宝收入||',income,'&&','支付宝支出||',pay) 
end bank_auxiliary_information,-- 支付宝到账辅助信息 

case when chitu_pay=0 and order_3150_chengben=0 then 'N' 
else 
concat(source,':赤兔赔付||',chitu_pay,'&&','3150赔付||',order_3150_chengben) 
end peifu_auxiliary_information,-- 赤兔和3150 赔付辅助信息 

case 
when Deduction_details is null then 'N' 
else 
Deduction_details
end quidao_auxiliary_information,-- 渠道扣款辅助信息

case when supplementary_amount is null then 'N' 
else 
supplementary_amount
end supplementary_auxiliary_information, -- 线下补款辅助信息

case when express_number is null then 'N' 
else express_number end first_express_number, -- 发货快递辅助信息

case 
when local_warehouse_ruku_time is null then 'N' 
else 
concat('入库时间||',local_warehouse_ruku_time,'&&','入库公司码||',local_warehouse_mjbarcode,'&&','退回快递||',refund_express_number) 
end local_warehouse_auxiliary_information, -- 入库辅助信息

case 
when zhuanji_time is null then 'N' 
else 
CONCAT('转寄时间||',zhuanji_time,'&&','转寄快递||',zhuanji_pickingExpressNumber) 
end zhuanji_auxiliary_information,-- 转寄辅助信息

case 
when zhuanji_return_time is null then 'N' 
else
CONCAT('转寄退回--退回时间||',zhuanji_time,'&&','退回公司码||',zhuanji_return_mjbarcode,'&&','退回快递||',zhuanji_return_pickingExpressNumber) 
end zhuanji_return_auxiliary_information, -- 转寄退回辅助信息

case 
when cainiao_category is null then 'N'
else concat('菜鸟仓入库') end cainiao_ruku_auxiliary_information,-- 菜鸟仓入库标记

case 
when cainiao_zhuanji_category is null then 'N' 
else concat('菜鸟转寄渠道') end cainiao_zhuanji_auxiliary_information, -- 菜鸟转寄标记

case when payAmount > 10 and (payamount+total_out)/payamount <0 and ruku_datediff is null and sales_channels not like '%批量退%' then '渠道亏损'
         when payAmount > 10 and (payamount+total_out)/payamount <0 and ruku_datediff <=90 and sales_channels not like '%批量退%'    then '本地仓三个月内入库亏损卖出' 
         when payAmount > 10 and (payamount+total_out)/payamount <0 and ruku_datediff >90 and (payAmount+total_out) <= -500 and sales_channels not like '%批量退%' then '本地仓超出三个月 但是亏损值大于500' end lable_fenlei 


from result_sale_order05;

update result_sale_order06 set chitu_pay =0 where chitu_pay is null;
update result_sale_order06 set chitu_pay =0 where sn != 1;
"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order06  聚合成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order06  聚合失败   ">> ${errologs}
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;
truncate table result_sale_order07;
insert into  result_sale_order07  
with test as (
select 
source,
bank_auxiliary_information,
peifu_auxiliary_information,
order_no,
JSON_ARRAY(
'order_no',order_no,
'first_express_number',first_express_number,
'qudao_chengben',quidao_auxiliary_information,
'supplementary',supplementary_auxiliary_information,
'local_warehouse',local_warehouse_auxiliary_information,
'zhuanji',zhuanji_auxiliary_information,
'zhuanji_return',zhuanji_return_auxiliary_information,
'cainiao_ruku',cainiao_ruku_auxiliary_information,
'cainiao_zhuanji',cainiao_zhuanji_auxiliary_information) remark 
from result_sale_order06 ) 

select source,group_concat(distinct bank_auxiliary_information) bank_auxiliary_information,group_concat(remark separator'&&') remark  from test 
group by source ;
"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order07  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order07  聚合失败   ">> ${errologs}
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "

SET GLOBAL group_concat_max_len=102400;
SET group_concat_max_len=102400;

update result_sale_order06 set good_status = '' where good_status is null;




update result_sale_order06 set good_status = '仅退款' where good_status like '%未发货退款%';

update result_sale_order06 set good_status = '退货退款' where good_status like '%已发货退款%';

update result_sale_order06 set good_status = '仅退款' where good_status like '%仅退款%';
update result_sale_order06 set good_status = '退货退款' where good_status like '%退货退款%';


update result_sale_order06 a,temu_新_消费者及履约保障_售后问题_欧洲 b 
set a.good_status = '退货退款' 
where a.source = b.备货单号;

update result_sale_order06 a,temu_新_消费者及履约保障_售后问题_全球 b 
set a.good_status = '退货退款' 
where a.source = b.备货单号;




truncate table  yingshou_yingfu_table;
insert into yingshou_yingfu_table 
select x.source,sum(x.zfb_yingshou_amount) zfb_yingshou_amount,sum(x.yingfu_chengben) yingfu_chengben from (
select  source,payamount zfb_yingshou_amount,-abs(costprice) yingfu_chengben from result_sale_order06 where good_status not like '%仅退款%' and  good_status not like '%退货退款%') x 
group by x.source;
"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " yingshou_yingfu_table  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " yingshou_yingfu_table  聚合失败   ">> ${errologs}
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "

truncate table result_sale_order08;
insert into result_sale_order08 

select a.*,b.remark,c.zfb_yingshou_amount,c.yingfu_chengben 
from result_sale_order06 a 
left join result_sale_order07 b on a.source=b.source 
left join yingshou_yingfu_table c on a.source=c.source

;

update result_sale_order08 set zfb_yingshou_amount =0 where  sn != 1;
update result_sale_order08 set yingfu_chengben =0 where  sn != 1;




--  速卖通 香港提货 运费  1574  元 平摊   175
-- 审批编号  202511271116000400379
-- 下面订单  回货完成  但是有额外运费


-- SO008918121     1115918480186427
-- SO008949771     1116053634817055
-- SO008967229     1116045294234023
-- SO009052472     8204956653100260
-- SO009098505     1116470746919612
-- SO009069805     1116340179272152
-- SO009005619     3061499002714123
-- SO009149658     1116590247531736
-- SO009081674     1116495597139250


update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1115918480186427' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116053634817055' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116045294234023' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '8204956653100260' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116470746919612' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116340179272152' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '3061499002714123' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116590247531736' and sn =1 ;
update result_sale_order08 set qudao_chengben = qudao_chengben - 175 where source = '1116495597139250' and sn =1 ;





-- temu 销售冲回   时间跨度太长  售后标签优化


truncate table temu_销售冲回订单明细;
insert into temu_销售冲回订单明细 
select distinct x.备货单号 from (
select 备货单号 from temu_新_入账明细_全球 where 交易类型 = '销售冲回'  and 备货单号 not in (select 备货单号 from temu_新_入账明细_全球 where 交易类型 = '非商责补贴' and 金额 > 20) union all 
select 备货单号 from temu_新_入账明细_欧洲 where 交易类型 = '销售冲回'  and 备货单号 not in (select 备货单号 from temu_新_入账明细_欧洲 where 交易类型 = '非商责补贴' and 金额 > 20) ) x ;

update result_sale_order08 a ,temu_销售冲回订单明细 b 
set a.good_status = '退货退款' 
where a.source = b.备货单号 and a.good_status = '';






"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order08  聚合成功    ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order08  聚合失败   ">> ${errologs}
    
fi


BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

SET GLOBAL group_concat_max_len=102400;
SET group_concat_max_len=102400;

-- 创建表09   补充快递状态信息  为了分类
drop table if exists result_sale_order09;
create table result_sale_order09 as 
select 
*,income+pay+xianxiabukuan+qudao_chengben+chitu_pay+order_3150+ruku-zhuanji+zhuanji_return+cainiao_ruku-cainiao_zhuanji_costprice profit,
'NNNNNNNNNN' state_first_express_number,
'NNNNNNNNNN' state_refund_express_number,
'NNNNNNNNNN' state_zhuanji_pickingExpressNumber,
'NNNNNNNNNN' state_zhuanji_return_pickingExpressNumber 
from result_sale_order08 

where 
sales_channels not like '%批量退%' and  
sales_channels not like '%退错要件%' and 
sales_channels not like '%内购%' and 
sales_channels not like '%转寄%' and 
sales_channels not like '%内购%' and 
sales_channels not like '%天马%' and 
sales_channels not like '%文石%' and 
sales_channels not like '%酷锐%' 


;


delete from result_sale_order09 where sn >1;


create index first_express_number on result_sale_order09(first_express_number);
alter table result_sale_order09 modify column refund_express_number varchar (100) not null;

create index refund_express_number on result_sale_order09(refund_express_number);
alter table result_sale_order09 modify column zhuanji_pickingExpressNumber varchar (100) not null;

update result_sale_order09 set zhuanji_return_pickingExpressNumber='' where zhuanji_return_pickingExpressNumber is null;

-- alter table result_sale_order09 modify column zhuanji_return_pickingExpressNumber varchar (100) not null;
-- create index zhuanji on result_sale_order09(zhuanji_pickingExpressNumber);
-- create index zhuanji_return on result_sale_order09(zhuanji_return_pickingExpressNumber);


truncate table standard_express_courier_state;
insert into standard_express_courier_state 
select standard_express,group_concat(distinct courier_state) courier_state from rugen_express_monitor group by standard_express;


update result_sale_order09 a,standard_express_courier_state b set a.state_first_express_number=b.courier_state where a.first_express_number=b.standard_express;
update result_sale_order09 a,standard_express_courier_state b set a.state_refund_express_number=b.courier_state where a.refund_express_number=b.standard_express;
update result_sale_order09 a,standard_express_courier_state b set a.state_zhuanji_pickingExpressNumber=b.courier_state where a.zhuanji_pickingExpressNumber=b.standard_express;
update result_sale_order09 a,standard_express_courier_state b set a.state_zhuanji_return_pickingExpressNumber=b.courier_state where a.zhuanji_return_pickingExpressNumber=b.standard_express;


update result_sale_order09 set state_first_express_number='' where state_first_express_number='NNNNNNNNNN';
update result_sale_order09 set state_refund_express_number='' where state_refund_express_number='NNNNNNNNNN';
update result_sale_order09 set state_zhuanji_pickingExpressNumber='' where state_zhuanji_pickingExpressNumber='NNNNNNNNNN';
update result_sale_order09 set state_zhuanji_return_pickingExpressNumber='' where state_zhuanji_return_pickingExpressNumber='NNNNNNNNNN';
update result_sale_order09 set profit =0 where  sn != 1;





"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " result_sale_order09  聚合成功    ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " result_sale_order09  聚合失败   ">> ${errologs}
    
fi






##########################################################################################################################
#中文描述：ERP销售单 基础表
#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202401
BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

drop table if exists rugen_result_sale_order;
create table rugen_result_sale_order as 

select 
'xxxxxxxxxxxxxxxxxxxxxxxxxxxxx' first_classification,
'xxxxxxxxxxxxxxxxxxxxxxxxxxxxx' secondary_classification,
'xxxx' hong_lv_deng,
'xxxxxxxxxx' result,
profit,  -- 订单利润
income,  -- 支付宝收入
pay,     -- 支付宝支出
chitu_pay,  -- 赤兔支出
qudao_chengben,   -- 渠道成本
xianxiabukuan,    -- 线下补款
ruku,             -- 入库金额
zhuanji,          -- 转寄金额
zhuanji_return,   -- 转寄退回金额
cainiao_ruku,     -- 菜鸟入库金额
cainiao_zhuanji_costprice,  -- 菜鸟仓转寄金额
order_3150_chengben,  -- 3150扣款

zfb_yingshou_amount,
yingfu_chengben,
datediff(now(),date_time) shijiancha,
datediff(now(),zhuanji_time) shijiancha_zhuanji,
date_time, -- 下单时间
order_no,-- so订单号
sale_channel_id,-- 店铺id
sales_channels,--  店铺名称
origin_state, -- 订单状态
ps_gx_order_state,-- 渠道状态
source,-- 基础单号
pickingSource,-- 平台
psGxWarehouseChannelName,-- 渠道
oneProductCode,-- 商家编码
platform_system_number,-- 单量
payAmount,-- 销售价
costprice,-- 成本价
is_refund,-- 是否支持退货
contact_phone,
buyer_open_uid,
oaid,
customer_name,
qudao_chengben + xianxiabukuan + ruku - zhuanji + zhuanji_return + cainiao_ruku - cainiao_zhuanji_costprice + income + pay total_out,-- 总支出
total_out_qudao,-- 渠道总支出 
order_3150,-- 3150支出
supplementary_amount,-- 线下补款
local_warehouse_category,-- 本地仓入库标识
local_warehouse_mjbarcode,-- 本地仓入库公司码
local_warehouse_costprice, -- 本地仓入库价格
zhuanji_time, -- 转寄时间
zhuanji_category, -- 本地仓转寄标识
zhuanji_costprice, -- 本地仓转寄金额

zhuanji_return_category, -- 转寄退回标记
zhuanji_return_costprice, -- 转寄退回金额 

cainiao_category, -- 菜鸟仓入库标识
cainiao_costprice, -- 菜鸟仓入库金额
cainiao_zhuanji_category, -- 菜鸟转寄标识
cainiao_zhuanji, -- 菜鸟转寄金额
refund_status, -- 售后状态
good_status, -- 退款类型
refund_local, -- 本地退标识
weifamiaotui,  -- 未发秒退标识
Deduction_details,
bank_auxiliary_information,
peifu_auxiliary_information,
supplementary_auxiliary_information,
remark,
state_first_express_number,
state_refund_express_number,
state_zhuanji_pickingExpressNumber,
state_zhuanji_return_pickingExpressNumber,
refund_express_number,zhuanji_pickingExpressNumber,'XXXXXXXXXXXXXXX' tongye_jiqiren_number 
from result_sale_order09 
where income+pay+qudao_chengben+xianxiabukuan+ruku-zhuanji+zhuanji_return+cainiao_ruku-cainiao_zhuanji_costprice <-50 -- 亏损50元以上订单

;

update rugen_result_sale_order set zfb_yingshou_amount= 0 where zfb_yingshou_amount is null;
update rugen_result_sale_order set yingfu_chengben= 0 where yingfu_chengben is null;

update rugen_result_sale_order set secondary_classification= '' ;
update rugen_result_sale_order set first_classification= '' ;
update rugen_result_sale_order set hong_lv_deng= '' ;
update rugen_result_sale_order set result= '' ;


delete from rugen_result_sale_order where payamount = 0;

-- 渠道重复扣款
update  rugen_result_sale_order 
set first_classification='',secondary_classification='' ;

-- 渠道重复扣款
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='渠道重复扣款' 
where 
payamount+total_out_qudao <=-50 and round(abs(payamount+total_out_qudao)/payamount,2)*100 >=30 AND 
pickingSource != 'MYJO' and pickingSource != 'CAINIAO';



-- 未发秒退订单
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='红' 
where weifamiaotui = '未发秒退'  and TIMESTAMPDIFF(hour,date_time,NOW())>120;


update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='红' 
where weifamiaotui = '极速退款' and TIMESTAMPDIFF(hour,date_time,NOW())>120;

 update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='黄' 
where weifamiaotui = '未发秒退' and TIMESTAMPDIFF(hour,date_time,NOW())<=120 and TIMESTAMPDIFF(hour,date_time,NOW())>72;


 update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='黄' 
where weifamiaotui = '极速退款' and TIMESTAMPDIFF(hour,date_time,NOW())<=120 and TIMESTAMPDIFF(hour,date_time,NOW())>72;


 update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='绿' 
where weifamiaotui = '未发秒退' and TIMESTAMPDIFF(hour,date_time,NOW())<=72;


 update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='未发秒退异常',hong_lv_deng='绿' 
where weifamiaotui = '极速退款' and TIMESTAMPDIFF(hour,date_time,NOW())<=72;



-- 转寄渠道未退款
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='转寄渠道未退款' 
where 
zhuanji >50 and 
ABS(qudao_chengben)>52 and 
ABS(qudao_chengben)-abs(yingfu_chengben)>=50 ;



update  rugen_result_sale_order 
set first_classification='支付宝类',secondary_classification='支付宝未收到货款' 
where 
secondary_classification='' 
and zfb_yingshou_amount >50 
and ABS(qudao_chengben) > 50 
and (income+pay)/zfb_yingshou_amount<0.6
and platform_system_number='单'  
and good_status = ''
;



update  rugen_result_sale_order 
set first_classification='支付宝类',secondary_classification='支付宝未收到货款' 
where 
secondary_classification='' and 
zfb_yingshou_amount >50 and 
ABS(qudao_chengben) > 50 and 
(income+pay)/zfb_yingshou_amount<0.8
and platform_system_number='多'  
and good_status = ''
;








-- 仅退款订单未退款
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='仅退款订单未退款' 
where 
secondary_classification='' AND 
ABS(qudao_chengben)>52 and 
ABS(qudao_chengben)-abs(yingfu_chengben)>=50 and 
pickingSource != 'MYJO' and 
good_status like '%仅退款%' 
;

-- 退货退款订单未退款
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='退货退款订单未退款' 
where 
secondary_classification='' AND 
ABS(qudao_chengben)>52 and 
ABS(qudao_chengben)-abs(yingfu_chengben)>=50 and 
pickingSource != 'MYJO' and 
(good_status like '%退货退款%' or good_status like '%已发货退款%')
;


-- 退货退款订单未退款
update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='退货退款订单未退款' 
where 
secondary_classification='' AND 
ABS(qudao_chengben)>52 and 
ABS(profit)>=50 and 
pickingSource != 'MYJO' and 
(good_status like '%退货退款%' or good_status like '%已发货退款%')
;







update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='定价异常' 
where 
secondary_classification='' and 
zfb_yingshou_amount >50 and 
ABS(qudao_chengben) > 50 and 
(income+pay)/zfb_yingshou_amount>=0.6
and platform_system_number='单'  
and pickingSource='MYJO'
;

update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='赔付' 
where 
secondary_classification='' and 
zfb_yingshou_amount >50 and 
ABS(qudao_chengben) > 50 and 
(income+pay)/zfb_yingshou_amount>=0.6
and platform_system_number='单'  
and pickingSource !='MYJO'
;







update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='定价异常' 
where 
secondary_classification='' 
and yingfu_chengben =0 and zfb_yingshou_amount=0 
and ABS(qudao_chengben)>52 
and pickingSource = 'MYJO' 
and income+pay >50;


update  rugen_result_sale_order 
set first_classification='本地仓类',secondary_classification='本地仓未收到货' 
where 
secondary_classification='' 
and yingfu_chengben =0 and zfb_yingshou_amount=0 
and ABS(qudao_chengben)>52 
and pickingSource = 'MYJO' 
and income+pay <50;


update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='转寄渠道未退款' 
where 
zhuanji >50 and 
ABS(qudao_chengben)>52 and 
profit <-50 

 ;

update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='赔付' 
where 
secondary_classification='' and 

qudao_chengben =0 

and (income+pay)<0

;

update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='赔付' 
where 
secondary_classification='' and 

abs(qudao_chengben)< 10

and (income+pay)<-49

;



update  rugen_result_sale_order 
set first_classification='支付宝类',secondary_classification='支付宝未收到货款' 
where 
secondary_classification='' 
and zfb_yingshou_amount >50 
and ABS(qudao_chengben) > 50 
and (income+pay)/zfb_yingshou_amount<0.6
and platform_system_number='单'  
and good_status = '换货'
;




update  rugen_result_sale_order 
set first_classification='渠道类',secondary_classification='退货退款订单未退款' 
where 
secondary_classification='' AND 
ABS(qudao_chengben)>49 and 
ABS(profit)>=50 
;




update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='赔付' 
where 
secondary_classification='' 
and yingfu_chengben =0 and zfb_yingshou_amount=0 ;

update  rugen_result_sale_order 
set first_classification='其他类',secondary_classification='赔付' 
where profit=pay;



update  rugen_result_sale_order
set first_classification='其他类',secondary_classification='赔付'
where secondary_classification='';




-- 红绿灯标记


update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='本地仓类' and 
secondary_classification='本地仓未收到货' and 
shijiancha>=15;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='本地仓类' and 
secondary_classification='本地仓未收到货' and 
shijiancha>=7 and shijiancha<15;

update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='本地仓类' and 
secondary_classification='本地仓未收到货' 
and hong_lv_deng='';



update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='支付宝类' and 
secondary_classification='支付宝未收到货款' and  
shijiancha>=30;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='支付宝类' and 
secondary_classification='支付宝未收到货款' and 
shijiancha>=15 and shijiancha<30;


update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='支付宝类' and 
secondary_classification='支付宝未收到货款' and 
hong_lv_deng='';



update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and 
secondary_classification='转寄渠道未退款' and 
shijiancha_zhuanji>=30;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and 
secondary_classification='转寄渠道未退款' and 
shijiancha_zhuanji>=15 and shijiancha_zhuanji<30;


update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='渠道类' and 
secondary_classification='转寄渠道未退款' 
and hong_lv_deng='';




update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and 
secondary_classification='渠道重复扣款' ;



update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and 
secondary_classification='仅退款订单未退款' and 
shijiancha>=30;


update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and 
secondary_classification='仅退款订单未退款' and 
shijiancha>=15 and shijiancha<30;


update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='渠道类' and 
secondary_classification='仅退款订单未退款' 
and hong_lv_deng='';




update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and 
secondary_classification='退货退款订单未退款' and 
shijiancha>=30;


update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and 
secondary_classification='退货退款订单未退款' and 
shijiancha>=15 and shijiancha<30;


update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='渠道类' and 
secondary_classification='退货退款订单未退款' 
and hong_lv_deng='';


update rugen_result_sale_order set result= '跟进中' where hong_lv_deng !='';


-- secondary_classification='本地仓未收到货'     hong_lv_deng= '黄' '绿'  不报异常

-- delete from rugen_result_sale_order where secondary_classification='本地仓未收到货' and hong_lv_deng= '黄';
-- delete from rugen_result_sale_order where secondary_classification='本地仓未收到货' and hong_lv_deng= '绿';

-- 仅退款订单未退款  退货退款订单未退款   时间差 7天（包含） 以内的不预警
-- delete from rugen_result_sale_order where secondary_classification='退货退款订单未退款'  and hong_lv_deng='绿'  and datediff(now(),date_time)<=7;
-- delete from rugen_result_sale_order where secondary_classification='仅退款订单未退款'  and hong_lv_deng='绿'  and datediff(now(),date_time)<=7;

-- secondary_classification='支付宝未收到货款'     hong_lv_deng= '黄' '绿'  不报异常

-- delete from rugen_result_sale_order where secondary_classification='支付宝未收到货款' and hong_lv_deng= '黄';
-- delete from rugen_result_sale_order where secondary_classification='支付宝未收到货款' and hong_lv_deng= '绿';

 
update rugen_result_sale_order set hong_lv_deng = '红' where date_time < '2026-03-01 00:00:00';
update rugen_result_sale_order set first_classification = '渠道类',secondary_classification = '退货退款订单未退款' where first_classification = '其他类' and date_time < '2026-03-01 00:00:00';
update rugen_result_sale_order set result = '跟进中' where result = ''  and date_time < '2026-03-01 00:00:00';

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " IP_ADDR   rugen_result_sale_order  打标签成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo "  IP_ADDR   rugen_result_sale_order  打标签失败   ">> ${errologs}
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
 mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -N -e " 
 use rugen;

 select distinct  * from rugen_result_sale_order where result= '跟进中' and first_classification !='其他类'

" >/data/exchange/rugen_result_sale_order.txt

 mysql -hRDS_HOST -ubig_data -pPASS -D big_data --local-infile -Bse "truncate table  rugen_result_sale_order_bak;load data local infile '/data/exchange/rugen_result_sale_order.txt' into table rugen_result_sale_order_bak character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " rugen_result_sale_order_bak  数据迁移成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " rugen_result_sale_order_bak  数据迁移失败   ">> ${errologs}
    
fi








##########################################################################################################################
#中文描述：-- 将最新计算得result_sale_order09 表 迁移到   正式库
#中文描述：-- 作用： 更新  rugen_result_sale_order 表 最新状态

#表单类型：普通表
#加工的库：研发原库
#加载方式: 数据表导出 
#开发人：DEV_NAME
#----------------------------------------------------------
#开发时间 ：202404

BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -e "
use rugen;

-- 将最新计算得result_sale_order09 表 迁移到   正式库
-- 作用： 更新  rugen_result_sale_order 表 最新状态

drop table if exists rugen_result_sale_order_update;
create table rugen_result_sale_order_update as 

select 
profit,  -- 订单利润
income,  -- 支付宝收入
pay,     -- 支付宝支出
chitu_pay,  -- 赤兔支出
qudao_chengben,   -- 渠道成本
xianxiabukuan,    -- 线下补款
ruku,             -- 入库金额
zhuanji,          -- 转寄金额
zhuanji_return,   -- 转寄退回金额
cainiao_ruku,     -- 菜鸟入库金额
cainiao_zhuanji_costprice, -- 菜鸟转寄金额
order_3150_chengben,  -- 3150扣款
zfb_yingshou_amount,
yingfu_chengben,
datediff(now(),date_time) shijiancha,
datediff(now(),zhuanji_time) shijiancha_zhuanji,
date_time, -- 下单时间
order_no,-- so订单号
sale_channel_id,-- 店铺id
sales_channels,--  店铺名称
origin_state, -- 订单状态
ps_gx_order_state,-- 渠道状态
source,-- 基础单号
pickingSource,-- 平台
psGxWarehouseChannelName,-- 渠道
oneProductCode,-- 商家编码
platform_system_number,-- 单量
payAmount,-- 销售价
costprice,-- 成本价
is_refund,-- 是否支持退货
contact_phone,
buyer_open_uid,
oaid,
customer_name,
qudao_chengben + xianxiabukuan + ruku - zhuanji + zhuanji_return + cainiao_ruku - cainiao_zhuanji_costprice + income + pay total_out,-- 总支出
total_out_qudao,-- 渠道总支出 
order_3150,-- 3150支出
supplementary_amount,-- 线下补款
local_warehouse_category,-- 本地仓入库标识
local_warehouse_mjbarcode,-- 本地仓入库公司码
local_warehouse_costprice, -- 本地仓入库价格
zhuanji_time, -- 转寄时间
zhuanji_category, -- 本地仓转寄标识
zhuanji_costprice, -- 本地仓转寄金额

zhuanji_return_category, -- 转寄退回标记
zhuanji_return_costprice, -- 转寄退回金额 

cainiao_category, -- 菜鸟仓入库标识
cainiao_costprice, -- 菜鸟仓入库金额
cainiao_zhuanji_category, -- 菜鸟转寄标识
cainiao_zhuanji, -- 菜鸟转寄金额
refund_status, -- 售后状态
good_status, -- 退款类型
refund_local, -- 本地退标识
weifamiaotui,  -- 未发秒退标识
Deduction_details,
bank_auxiliary_information,
peifu_auxiliary_information,
supplementary_auxiliary_information,
remark,
state_first_express_number,
state_refund_express_number,
state_zhuanji_pickingExpressNumber,
state_zhuanji_return_pickingExpressNumber,
refund_express_number,zhuanji_pickingExpressNumber,'XXXXXXXXXXXXXXX' tongye_jiqiren_number 
from result_sale_order09 

;


update rugen_result_sale_order_update set cainiao_zhuanji_costprice  = 0 where cainiao_zhuanji_costprice is null;

"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " rugen_result_sale_order_update 计算 成功  ">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}    
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " rugen_result_sale_order_update 计算 失败   ">> ${errologs}
    
fi







BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
 mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -N -e " 
 use rugen;

 select distinct  * from rugen_result_sale_order_update 

" >/data/exchange/rugen_result_sale_order_update.txt

 mysql -hRDS_HOST -ubig_data -pPASS -D big_data --local-infile -Bse "truncate table  rugen_result_sale_order_update;load data local infile '/data/exchange/rugen_result_sale_order_update.txt' into table rugen_result_sale_order_update character set utf8mb4 fields terminated by '\t' lines terminated by '\n';"


if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " rugen_result_sale_order_update  迁移成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " rugen_result_sale_order_update  迁移失败   ">> ${errologs}
    
fi





BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

 mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -N -e " 
 use rugen;

 select * from 下单日志;

" >/data/exchange/下单日志.txt

 mysql -hRDS_HOST -ubig_data -pPASS -D big_data --local-infile -Bse "truncate table  下单日志;load data local infile '/data/exchange/下单日志.txt' into table 下单日志 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

delete from 下单日志 where ERP订单号 not like 'SO%';
delete from 下单日志 where 下单状态 = '下单失败';
update 下单日志 set ERP订单号 = substring(ERP订单号,1,11);

truncate table 下单日志01;
insert into 下单日志01 
select ERP订单号,substring_index(group_concat(distinct 手机名称),',',1) 手机名称 from 下单日志 group by ERP订单号;
 "

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 下单日志 成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 下单日志 失败   ">> ${errologs}
    
fi



BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
use big_data;
set innodb_lock_wait_timeout = 500;

update rugen_result_sale_order set cainiao_zhuanji_costprice  = 0 where cainiao_zhuanji_costprice is null;

insert into rugen_result_sale_order 
(
first_classification
,secondary_classification
,hong_lv_deng
,result
,profit
,income
,pay
,chitu_pay
,qudao_chengben
,xianxiabukuan
,ruku
,zhuanji
,zhuanji_return
,cainiao_ruku
,cainiao_zhuanji_costprice
,order_3150_chengben
,zfb_yingshou_amount
,yingfu_chengben
,date_time
,order_no
,sale_channel_id
,sales_channels
,origin_state
,ps_gx_order_state
,source
,pickingSource
,psGxWarehouseChannelName
,oneProductCode
,platform_system_number
,payAmount
,costprice
,is_refund
,contact_phone
,buyer_open_uid
,oaid
,customer_name
,total_out
,total_out_qudao
,order_3150
,supplementary_amount
,local_warehouse_category
,local_warehouse_mjbarcode
,local_warehouse_costprice
,zhuanji_category
,zhuanji_costprice
,zhuanji_return_category
,zhuanji_return_costprice
,cainiao_category
,cainiao_costprice
,cainiao_zhuanji_category
,cainiao_zhuanji
,refund_status
,good_status
,refund_local
,weifamiaotui
,Deduction_details
,bank_auxiliary_information
,peifu_auxiliary_information
,supplementary_auxiliary_information
,remark
,create_time
,after_sales_lable 

) 
select 
first_classification
,secondary_classification
,hong_lv_deng
,result
,profit
,income
,pay
,chitu_pay
,qudao_chengben
,xianxiabukuan
,ruku
,zhuanji
,zhuanji_return
,cainiao_ruku
,cainiao_zhuanji_costprice
,order_3150_chengben
,zfb_yingshou_amount
,yingfu_chengben
,date_time
,order_no
,sale_channel_id
,sales_channels
,origin_state
,ps_gx_order_state
,source
,pickingSource
,psGxWarehouseChannelName
,oneProductCode
,platform_system_number
,payAmount
,costprice
,is_refund
,contact_phone
,buyer_open_uid
,oaid
,customer_name
,total_out
,total_out_qudao
,order_3150
,supplementary_amount
,local_warehouse_category
,local_warehouse_mjbarcode
,local_warehouse_costprice
,zhuanji_category
,zhuanji_costprice
,zhuanji_return_category
,zhuanji_return_costprice
,cainiao_category
,cainiao_costprice
,cainiao_zhuanji_category
,cainiao_zhuanji
,refund_status
,good_status
,refund_local
,weifamiaotui
,Deduction_details
,bank_auxiliary_information
,peifu_auxiliary_information
,supplementary_auxiliary_information
,remark,now() create_time,'其它' after_sales_lable 
from rugen_result_sale_order_bak 

where date_time >= '2026-03-01 00:00:00' and 
source not in (select distinct source from rugen_result_sale_order);

update rugen_result_sale_order set state_first_express_number='' where state_first_express_number is null;
update rugen_result_sale_order set state_refund_express_number='' where state_refund_express_number is null;
update rugen_result_sale_order set state_zhuanji_pickingExpressNumber='' where state_zhuanji_pickingExpressNumber is null;
update rugen_result_sale_order set state_zhuanji_return_pickingExpressNumber='' where state_zhuanji_return_pickingExpressNumber is null;
update rugen_result_sale_order set state_first_express_number='' where state_first_express_number = '暂无';
update rugen_result_sale_order set state_refund_express_number='' where state_refund_express_number = '暂无';
update rugen_result_sale_order set state_zhuanji_pickingExpressNumber='' where state_zhuanji_pickingExpressNumber = '暂无';
update rugen_result_sale_order set state_zhuanji_return_pickingExpressNumber='' where state_zhuanji_return_pickingExpressNumber = '暂无';

update rugen_result_sale_order a ,rugen_result_sale_order_bak b 
set 
a.state_first_express_number=b.state_first_express_number,
a.state_refund_express_number=b.state_refund_express_number,
a.state_zhuanji_pickingExpressNumber=b.state_zhuanji_pickingExpressNumber,
a.state_zhuanji_return_pickingExpressNumber=b.state_zhuanji_return_pickingExpressNumber 
where a.order_no=b.order_no;



-- 红绿灯标识更新
-- 未发秒退订单
update  rugen_result_sale_order 
set hong_lv_deng='红' 
where first_classification='渠道类' and secondary_classification='未发秒退异常'  and result = '跟进中' and TIMESTAMPDIFF(hour,date_time,NOW())>240;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where first_classification='渠道类' and secondary_classification='未发秒退异常'  and result = '跟进中' and TIMESTAMPDIFF(hour,date_time,NOW())<=240 and TIMESTAMPDIFF(hour,date_time,NOW())>120;;

update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where first_classification='渠道类' and secondary_classification='未发秒退异常'  and result = '跟进中' and TIMESTAMPDIFF(hour,date_time,NOW())<=120;


update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='本地仓类' and result = '跟进中' and 
secondary_classification='本地仓未收到货' and 
datediff(now(),date_time)>=30;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='本地仓类' and  result = '跟进中' and 
secondary_classification='本地仓未收到货' and 
datediff(now(),date_time)>=15 and datediff(now(),date_time)<30;


update  rugen_result_sale_order 
set hong_lv_deng='绿' 
where 
first_classification='本地仓类' and  result = '跟进中' and 
secondary_classification='本地仓未收到货' and 
datediff(now(),date_time)<15;





update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='支付宝类' and  result = '跟进中' and 
secondary_classification='支付宝未收到货款' and  
datediff(now(),date_time)>=30;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='支付宝类' and  result = '跟进中' and 
secondary_classification='支付宝未收到货款' and 
datediff(now(),date_time)>=15 and datediff(now(),date_time)<30;





update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='转寄渠道未退款' and 
datediff(now(),date_time)>=30;

update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='转寄渠道未退款' and 
datediff(now(),date_time)>=15 and datediff(now(),date_time)<30;





update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='渠道重复扣款' ;



update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='仅退款订单未退款' and 
datediff(now(),date_time)>=30;


update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='仅退款订单未退款' and 
datediff(now(),date_time)>=15 and datediff(now(),date_time)<30;






update  rugen_result_sale_order 
set hong_lv_deng='红' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='退货退款订单未退款' and 
datediff(now(),date_time)>=30;


update  rugen_result_sale_order 
set hong_lv_deng='黄' 
where 
first_classification='渠道类' and  result = '跟进中' and 
secondary_classification='退货退款订单未退款' and 
datediff(now(),date_time)>=15 and datediff(now(),date_time)<30;





delete from  rugen_result_sale_order where profit=pay+chitu_pay+order_3150_chengben and  date_time >= '2026-03-01 00:00:00' ;


delete from  rugen_result_sale_order where profit>-50 and date_time >= '2026-03-01 00:00:00' ;



delete from  rugen_result_sale_order where profit=pay and  date_time >= '2026-03-01 00:00:00' ;




-- 完结预警时间新
update rugen_result_sale_order set qiangzhi_wanjie_yujing_time=TIMESTAMPDIFF(day,date_time,NOW());

delete from rugen_result_sale_order where hong_lv_deng='自动校验';
"



if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " 新增 ETL调度订单rugen_result_sale_order  成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 新增 ETL调度订单rugen_result_sale_order  失败   ">> ${errologs}
    
fi





BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
use big_data;
update rugen_result_sale_order a,rugen_result_sale_order_update b 
set 
 a.profit = b.profit
,a.income = b.income
,a.pay = b.pay
,a.chitu_pay = b.chitu_pay
,a.qudao_chengben = b.qudao_chengben
,a.xianxiabukuan = b.xianxiabukuan
,a.ruku = b.ruku
,a.total_out_qudao=b.total_out_qudao
,a.zhuanji = b.zhuanji
,a.zhuanji_return = b.zhuanji_return
,a.cainiao_ruku = b.cainiao_ruku
,a.cainiao_zhuanji_costprice = b.cainiao_zhuanji_costprice
,a.order_3150_chengben = b.order_3150_chengben
,a.source = b.source
,a.remark = b.remark
,a.Deduction_details = b.Deduction_details
,a.bank_auxiliary_information = b.bank_auxiliary_information
,a.peifu_auxiliary_information = b.peifu_auxiliary_information
,a.supplementary_auxiliary_information = b.supplementary_auxiliary_information 
,a.refund_express_number=b.refund_express_number 
,a.zhuanji_pickingExpressNumber=b.zhuanji_pickingExpressNumber
,a.tongye_jiqiren_number=b.tongye_jiqiren_number 
where a.source=b.source
and a.date_time >= '2026-02-01 00:00:00'   
;


update rugen_result_sale_order set hong_lv_deng = '红' where date_time <= '2026-03-01 00:00:00' ;



-- 这一步很重要 就是记录  ETL调度自动审核完结订单
/*
drop table if exists ETL调度自动审核完结订单_$year$month$day;
create table ETL调度自动审核完结订单_$year$month$day as  
select a.* from (
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where (chitu_pay + order_3150_chengben)/if(profit=0,0.1,profit) >= 0.5  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where (income+pay)>45 and pickingSource='MYJO' and platform_system_number = '单'  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where (income+pay)>45 and pickingSource='CAINIAO' and platform_system_number = '单'  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where (income+pay) / if(profit=0,0.1,profit) >=0.8  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where (qudao_chengben + ruku - zhuanji + zhuanji_return + cainiao_ruku - cainiao_zhuanji_costprice) >=-50.3  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where ((income+pay)/if(qudao_chengben=0,0.1,abs(qudao_chengben))>=0.7 and platform_system_number = '单')  union all
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where pay != 0 and profit/pay >0.7 and profit/pay <1.2 and income + pay >45 union all  
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where qudao_chengben >=-50  union all 
select source,order_no,date_time,sales_channels,sale_channel_id from rugen_result_sale_order where ((income+pay)/if(qudao_chengben=0,0.1,abs(qudao_chengben))>=0.8 and platform_system_number = '多')) a ;

*/





delete from rugen_result_sale_order where source in (select a.source from (
select source from rugen_result_sale_order where (chitu_pay + order_3150_chengben)/if(profit=0,0.1,profit) >= 0.5  union all 
select source from rugen_result_sale_order where (income+pay)>45 and pickingSource='MYJO' and platform_system_number = '单'  union all 
select source from rugen_result_sale_order where (income+pay)>45 and pickingSource='CAINIAO' and platform_system_number = '单'  union all 
select source from rugen_result_sale_order where (income+pay) / if(profit=0,0.1,profit) >=0.8  union all 
select source from rugen_result_sale_order where (qudao_chengben + ruku - zhuanji + zhuanji_return + cainiao_ruku - cainiao_zhuanji_costprice) >=-50.3  union all 
select source from rugen_result_sale_order where ((income+pay)/if(qudao_chengben=0,0.1,abs(qudao_chengben))>=0.7 and platform_system_number = '单')  union all
select source from rugen_result_sale_order where pay != 0 and profit/pay >0.7 and profit/pay <1.2 and income + pay >45 union all  
select source from rugen_result_sale_order where qudao_chengben >=-50  union all 
select source from rugen_result_sale_order where ((income+pay)/if(qudao_chengben=0,0.1,abs(qudao_chengben))>=0.8 and platform_system_number = '多')) a )
and  date_time >= '2026-03-01 00:00:00' ;
;

delete from rugen_result_sale_order where profit >= -40 and  date_time >= '2026-03-01 00:00:00' ;



-- secondary_classification='支付宝未收到货款'     hong_lv_deng= '黄' '绿'  不报异常

delete from rugen_result_sale_order where secondary_classification='支付宝未收到货款' and hong_lv_deng= '黄';
delete from rugen_result_sale_order where secondary_classification='支付宝未收到货款' and hong_lv_deng= '绿';


-- secondary_classification='本地仓未收到货'     hong_lv_deng= '黄' '绿'  不报异常

delete from rugen_result_sale_order where secondary_classification='本地仓未收到货' and hong_lv_deng= '黄';
delete from rugen_result_sale_order where secondary_classification='本地仓未收到货' and hong_lv_deng= '绿';

-- 仅退款订单未退款  退货退款订单未退款   时间差 7天（包含） 以内的不预警
delete from rugen_result_sale_order where secondary_classification='退货退款订单未退款'  and hong_lv_deng='绿'  and datediff(now(),date_time)<=7;
delete from rugen_result_sale_order where secondary_classification='仅退款订单未退款'  and hong_lv_deng='绿'  and datediff(now(),date_time)<=7;

-- 渠道重复扣款  时间差 3天（包含） 以内的不预警

-- delete from rugen_result_sale_order where secondary_classification='渠道重复扣款 '  and datediff(now(),date_time)<=3;

delete from rugen_result_sale_order where result = '跟进中' and secondary_classification='渠道重复扣款' and round(abs(payamount+total_out_qudao)/payamount,2)*100 <30 and  date_time >= '2026-03-01 00:00:00' ;


update  rugen_result_sale_order set refund_express_number = '' where refund_express_number is null;
update  rugen_result_sale_order set refund_express_number = '' where refund_express_number='N';

update  rugen_result_sale_order set zhuanji_pickingExpressNumber = '' where zhuanji_pickingExpressNumber is null;
update  rugen_result_sale_order set tongye_jiqiren_number = '' ;

update rugen_result_sale_order a,下单日志01 b set a.tongye_jiqiren_number=b.手机名称 where a.order_no = b.ERP订单号;




update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6941232237407770009' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4290907467594520540' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6918997800109374880' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4319998812839544819' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '2529714974874550872' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4310180751879670109' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6941210067729323823' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4290136167019738111' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6941443093344621932' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6941226535464932910' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '6919178247291174440' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4314599929276504035' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '4286546858251701849' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '2527609296141311562' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '2520067191746126996' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '2543604171279507290' and result not like '%完结%';
update rugen_result_sale_order  set first_classification='渠道类' ,secondary_classification= '退货退款订单未退款',hong_lv_deng = '红',result= '跟进中' where source = '314272170538' and result not like '%完结%';






"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " rugen_result_sale_order  数据更新成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " rugen_result_sale_order  数据更新失败 ">> ${errologs}
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
use big_data;
-- 订单处理人更新

update rugen_result_sale_order a , ETL调度订单处理人 b set a.processor=b.order_processor where  a.processor is null and a.sales_channels=b.shop_name;

update rugen_result_sale_order a , ETL调度订单处理人 b set a.processor=b.order_processor where  a.processor = '' and a.sales_channels=b.shop_name;

update rugen_result_sale_order a , ETL调度订单处理人 b set a.processor=b.order_processor where  a.processor = '无'  and a.sales_channels=b.shop_name;

update rugen_result_sale_order a , ETL调度订单处理人 b set a.processor=b.order_processor where  a.processor = '超级管理员' and a.sales_channels=b.shop_name;


-- 将  退货退款订单未退款   这一类异常订单  判断是否有转寄行为  修改异常类型   转寄渠道未退款
update  rugen_result_sale_order set secondary_classification='转寄渠道未退款' where  secondary_classification='退货退款订单未退款'  and result = '跟进中' and zhuanji >50 ;




-- 售后标签自定义标签恢复   只保留已经定义的售后标签 自定义标签 复位
update rugen_result_sale_order set after_sales_lable = '其它' where 
after_sales_lable !='小二判退'
and after_sales_lable !='转寄渠道拒收'
and after_sales_lable !='紧急追款追货'
and after_sales_lable !='渠道重复扣款'
and after_sales_lable !='退回渠道等待退款'
and after_sales_lable !='渠道待转寄本地仓'
and after_sales_lable !='已标记本地退待本地仓入库'
and after_sales_lable !='订单售后中'
and after_sales_lable !='未发秒退'
and after_sales_lable !='换货'
and after_sales_lable !='疑难订单'
and after_sales_lable !='快递丢件待赔付' 
and after_sales_lable !='等待买家确认收货'
and result = '跟进中';





-- 淘宝处理人 重新分配

update rugen_result_sale_order set processor = '麦朵' where sales_channels like '%快手%' and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '抖店-时禾美运动鞋服专营店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦朵' where sales_channels = '抖店-万源' and result = '跟进中';
update rugen_result_sale_order set processor = '麦朵' where sales_channels = '抖店万源运动鞋服专营店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦朵' where sales_channels = '抖店-万源运动专营店' and result = '跟进中';

update rugen_result_sale_order set processor = '麦朵' where sales_channels = '抖店-万源户外鞋服专营店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦朵' where sales_channels = '抖店-万源鞋服专营店' and result = '跟进中';

update rugen_result_sale_order set processor = '麦朵' where sales_channels like '抖店%' and sales_channels like '%子铁%' and result = '跟进中';



update rugen_result_sale_order set processor = '麦一' where sales_channels = '肥猫体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '角马体育正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦云' where sales_channels = '鲨鱼运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-Ace体育SNEAKER店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-GoGoSneaker店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-JackWolfskin品牌折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-OG体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-STAR.P.E正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-TheEagles飞鹰体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-YOYO体育Sneaker' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-白龙运动折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-百步店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-北极熊正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-比邻星体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-彼博体育店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-冰锤户外' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-城市屋顶折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-晟风体育店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-驰骋体育super店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-驰耀体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝冲鸭店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-达达正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-大象户外综合店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-地平线SNEAKER' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-独角兽体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-梵语体育SNEAKER' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-飞渡运动综合折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-飞鱼运动集合' and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-风街店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-烽行运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-孤客体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-古德国潮体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-国潮港湾正品店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-海川店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-河马体育SNEAKER店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-宏越体育正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-火狐运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-火云鞋神正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-疾星运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-佳悦奥莱折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-江河体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-君采体育正品' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-凯威运动折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-蓝鲸体育正品店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-澜图运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-乐动运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-雷动店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-棱镜一站户外折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-凌跃正品运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-零号体育SNEAKER' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-龙卷风运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-卢克体育店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-芒芒运动正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦云' where sales_channels = '淘宝-萌神店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-摩登天空运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-欧乐运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-破浪店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-奇凡鞋柜' and result = '跟进中';
update rugen_result_sale_order set processor = '麦云' where sales_channels = '淘宝-启扬店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-起点运动正品店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-晴天正品运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-曲奇运动' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-尚品体育正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-深蓝Sneaker' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-神猫运动集合' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-世风国货体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-朔风体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-桃花岛体育正品折扣店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-淘淘正品体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '淘宝-薇薇体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-维卡type正品店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-五湖店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-先驱正品' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-向尚体育正品国货店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-鞋霸正品集合店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-鞋星人' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-新世纪正品国货' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-星琪体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-星熠体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-雅诚sports国货正品' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-雅斯菲体育店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦圈' where sales_channels = '淘宝-焱鑫店' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-扬帆体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-影豹体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦一' where sales_channels = '淘宝-硬汉体育Sneaker' and result = '跟进中';
update rugen_result_sale_order set processor = '麦缇' where sales_channels = '淘宝-跃尚体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-跃卓国货体育' and result = '跟进中';
update rugen_result_sale_order set processor = '麦宋' where sales_channels = '大猩猩运动正品折扣店' and result = '跟进中';




update rugen_result_sale_order set processor = '麦浩' where sales_channels = '大猩猩运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-STAR.P.E正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-白龙运动折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-北极熊正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-比邻星体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-城市屋顶折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-晟风体育店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-驰骋体育super店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-驰耀体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝冲鸭店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-独角兽体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-飞鱼运动集合' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-风街店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-烽行运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-古德国潮体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-国潮港湾正品店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-火狐运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-疾星运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-江河体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-君采体育正品' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-凯威运动折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-蓝鲸体育正品店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-澜图运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-乐动运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-凌跃正品运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-龙卷风运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-摩登天空运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-奇凡鞋柜' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-起点运动正品店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-世风国货体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-朔风体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-桃花岛体育正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-维卡type正品店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-五湖店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-先驱正品' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-向尚体育正品国货店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-新世纪正品国货' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-星熠体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-雅诚sports国货正品' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-雅斯菲体育店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-焱鑫店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-影豹体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-硬汉体育Sneaker' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-跃卓国货体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '肥猫体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '角马体育正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '鲨鱼运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-Ace体育SNEAKER店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-GoGoSneaker店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-JackWolfskin品牌折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-OG体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-TheEagles飞鹰体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-YOYO体育Sneaker' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-百步店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-彼博体育店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-冰锤户外' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-达达正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-大象户外综合店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-地平线SNEAKER' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-梵语体育SNEAKER' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-飞渡运动综合折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-孤客体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-海川店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-河马体育SNEAKER店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-宏越体育正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-火云鞋神正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-佳悦奥莱折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-雷动店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-棱镜一站户外折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-零号体育SNEAKER' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-卢克体育店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-芒芒运动正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-萌神店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-欧乐运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-破浪店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-启扬店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-晴天正品运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-曲奇运动' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-尚品体育正品折扣店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-深蓝Sneaker' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-神猫运动集合' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-淘淘正品体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-薇薇体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-鞋霸正品集合店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '淘宝-鞋星人' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-星琪体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-扬帆体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '淘宝-跃尚体育' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '抖店-时禾美运动鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-万源' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店万源运动鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-万源运动专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';

update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店-子铁体育专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-子铁运动鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-子铁鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-子铁运动专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-子铁户外鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店-万源鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';


update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店-万源户外鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';


update rugen_result_sale_order set processor='麦斐3' where  sales_channels like '%拼多多%' and sales_channels like '%尚玄%';
update rugen_result_sale_order set processor='麦叮1' where  sales_channels like '%抖店%' and sales_channels like '%尚玄%';


update rugen_result_sale_order set processor = '麦斐3' where sales_channels = '淘宝-STAR.P.E正品折扣店' and result = '跟进中' and date_time > '2025-10-27 00:00:00';
update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '淘宝-风街店' and result = '跟进中' and date_time > '2025-10-27 00:00:00';




update rugen_result_sale_order set processor = '麦静1' where processor = '麦静' and  result = '跟进中';
update rugen_result_sale_order set processor = '麦云1' where processor = '麦云' and  result = '跟进中';


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo " rugen_result_sale_order  ETL调度处理人重新分配 淘宝  成功  ">> ${logs}
     echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}   
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " rugen_result_sale_order  ETL调度处理人重新分配 淘宝  失败 ">> ${errologs}
    
fi




BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
use big_data;
--  综合天猫 按照标签 店铺 匹配分配人



-- 订单处理人更新  重新分配  
-- 天猫

update rugen_result_sale_order set processor = '麦欣' where result = '跟进中' and sales_channels like '%天猫%';

-- 拼多多
update rugen_result_sale_order set processor = '麦袜' where result = '跟进中' and sales_channels like '%拼多多%';


-- 京东
update rugen_result_sale_order set processor = '麦优' where result = '跟进中' and sales_channels like '%京东%';


-- 抖店
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-公司';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-公司运动专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-公司鞋服专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-公司户外鞋服专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-实洋鞋服专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-实洋运动鞋服专营店';

update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-实洋运动专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-实洋体育专营店';
update rugen_result_sale_order set processor = '麦巧' where result = '跟进中' and sales_channels = '抖店-实洋户外鞋服专营店';




update rugen_result_sale_order set processor = '星麦2' 
where 
processor = '麦巧'  and 
(after_sales_lable ='小二判退' or after_sales_lable ='转寄渠道拒收' or after_sales_lable ='紧急追款追货' or after_sales_lable ='渠道重复扣款' or after_sales_lable ='退回渠道等待退款' or after_sales_lable ='渠道待转寄本地仓' or after_sales_lable ='快递丢件待赔付' or after_sales_lable = '疑难订单') 
and result = '跟进中';

-- update rugen_result_sale_order set processor = '麦涛' where (sales_channels = '抖店-实洋运动鞋服专营店' or sales_channels = '抖店-实洋鞋服专营店' or '抖店-公司户外鞋服专营店' or '抖店-万源户外鞋服专营店' or '抖店-万源鞋服专营店' or '抖店-公司鞋服专营店')  and after_sales_lable ='已标记本地退待本地仓入库' and result = '跟进中';




-- 小二判退

update rugen_result_sale_order set processor='麦端' where after_sales_lable ='小二判退' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='小二判退' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='小二判退' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='小二判退' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='小二判退' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='小二判退' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';




update rugen_result_sale_order set processor='麦端' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='转寄渠道拒收' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';




update rugen_result_sale_order set processor='麦端' where after_sales_lable ='紧急追款追货' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='紧急追款追货' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='紧急追款追货' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='紧急追款追货' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='紧急追款追货' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='紧急追款追货' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';


update rugen_result_sale_order set processor='麦欣' where after_sales_lable ='渠道重复扣款' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦袜' where after_sales_lable ='渠道重复扣款' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦优' where after_sales_lable ='渠道重复扣款' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='渠道重复扣款' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='渠道重复扣款' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='渠道重复扣款' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';




update rugen_result_sale_order set processor='麦端' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='退回渠道等待退款' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';


update rugen_result_sale_order set processor='麦端' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='渠道待转寄本地仓' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';



-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%天猫%' and result = '跟进中';
-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%拼多多%' and result = '跟进中';
-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%京东%' and result = '跟进中';
-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%抖店-公司%' and result = '跟进中';
-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%抖店公司%' and result = '跟进中';
-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';


update rugen_result_sale_order set processor='麦欣' where after_sales_lable ='订单售后中' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦袜' where after_sales_lable ='订单售后中' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦优' where after_sales_lable ='订单售后中' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='订单售后中' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='订单售后中' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='订单售后中' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';


update rugen_result_sale_order set processor='麦欣' where after_sales_lable ='等待买家确认收货' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦袜' where after_sales_lable ='等待买家确认收货' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦优' where after_sales_lable ='等待买家确认收货' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='等待买家确认收货' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='等待买家确认收货' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='麦巧' where after_sales_lable ='等待买家确认收货' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';





update rugen_result_sale_order set processor='麦端' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%天猫%' and result = '跟进中';
update rugen_result_sale_order set processor='麦正' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%拼多多%' and result = '跟进中';
update rugen_result_sale_order set processor='麦钰' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%京东%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%抖店-公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%抖店公司%' and result = '跟进中';
update rugen_result_sale_order set processor='星麦' where after_sales_lable ='快递丢件待赔付' and sales_channels like '%抖店-尚玄户外鞋服专营店%' and result = '跟进中';


-- 疑难订单 分配人
update rugen_result_sale_order set processor = '麦端' where result = '跟进中' and sales_channels like '%天猫%' and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '麦正' where result = '跟进中' and sales_channels like '%拼多多%'  and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '麦钰' where result = '跟进中' and sales_channels like '%京东%'  and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '星麦' where result = '跟进中' and sales_channels = '抖店-公司'  and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '星麦' where result = '跟进中' and sales_channels = '抖店-公司运动专营店'  and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '星麦' where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店'  and after_sales_lable = '疑难订单';
update rugen_result_sale_order set processor = '星麦' where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店'  and after_sales_lable = '疑难订单';






-- 未发秒退异常  自动打标签  未发秒退标识 并分配处理人

update rugen_result_sale_order set processor = '麦端',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels like '%天猫%' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';

update rugen_result_sale_order set processor = '麦正',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels like '%拼多多%' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';


update rugen_result_sale_order set processor = '麦钰',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels like '%京东%' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';


update rugen_result_sale_order set processor = '星麦',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels = '抖店-公司' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels = '抖店-公司运动专营店' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='未发秒退' where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店' and secondary_classification='未发秒退异常' and after_sales_lable = '其它';




update rugen_result_sale_order set processor = '麦端'  where result = '跟进中' and sales_channels like '%天猫%' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '麦正'  where result = '跟进中' and sales_channels like '%拼多多%' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '麦钰'  where result = '跟进中' and sales_channels like '%京东%' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '星麦'  where result = '跟进中' and sales_channels = '抖店-公司' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '星麦'  where result = '跟进中' and sales_channels = '抖店-公司运动专营店' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '星麦'  where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '星麦'  where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店' and after_sales_lable='未发秒退' ;



-- 换货 标签   分配人  规范标签

update rugen_result_sale_order set processor='麦端' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%天猫%';
update rugen_result_sale_order set processor='星麦' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%抖店%';
update rugen_result_sale_order set processor='麦钰' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%京东%';
update rugen_result_sale_order set processor='麦正' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%拼多多%';

update rugen_result_sale_order set processor='麦端',after_sales_lable='换货' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%天猫%' and after_sales_lable = '其它';
update rugen_result_sale_order set processor='星麦',after_sales_lable='换货' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%抖店%' and after_sales_lable = '其它';
update rugen_result_sale_order set processor='麦钰',after_sales_lable='换货' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%京东%' and after_sales_lable = '其它';
update rugen_result_sale_order set processor='麦正',after_sales_lable='换货' where Deduction_details like '%换货%' and result = '跟进中' and sales_channels like '%拼多多%' and after_sales_lable = '其它';

update rugen_result_sale_order set processor='麦端' where result = '跟进中' and after_sales_lable = '换货' and sales_channels like '%天猫%';
update rugen_result_sale_order set processor='星麦' where result = '跟进中' and after_sales_lable = '换货' and sales_channels like '%抖店%';
update rugen_result_sale_order set processor='麦钰' where result = '跟进中' and after_sales_lable = '换货' and sales_channels like '%京东%';
update rugen_result_sale_order set processor='麦正' where result = '跟进中' and after_sales_lable = '换货' and sales_channels like '%拼多多%';



-- 转寄渠道未退款异常  自动打标签 分配处理人
update rugen_result_sale_order set processor = '麦端',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels like '%天猫%' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦正',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels like '%拼多多%' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦钰',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels like '%京东%' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels = '抖店-公司' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels = '抖店-公司运动专营店' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '星麦',after_sales_lable='退回渠道等待退款' where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店' and secondary_classification='转寄渠道未退款' and after_sales_lable = '其它';


-- 支付宝未收到货款异常 自动打标签  分配处理人

update rugen_result_sale_order set processor = '麦欣',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels like '%天猫%' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦袜',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels like '%拼多多%' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦优',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels like '%京东%' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦巧',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels = '抖店-公司' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦巧',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels = '抖店-公司运动专营店' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦巧',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels = '抖店公司运动鞋服专营店' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';
update rugen_result_sale_order set processor = '麦巧',after_sales_lable='等待买家确认收货' where result = '跟进中' and sales_channels = '抖店-尚玄户外鞋服专营店' and secondary_classification='支付宝未收到货款' and after_sales_lable = '其它';


update rugen_result_sale_order set processor = '星麦2' where processor = '星麦';



update  rugen_result_sale_order set processor='星麦2' where  processor='麦巧' and  result = '跟进中' and  (secondary_classification='未发秒退异常' or secondary_classification='转寄渠道未退款' or secondary_classification='退货退款订单未退款' or secondary_classification='仅退款订单未退款');

update rugen_result_sale_order set processor = '星麦2'  where result = '跟进中' and processor='麦巧' and after_sales_lable='未发秒退' ;
update rugen_result_sale_order set processor = '星麦2' where processor = '星麦';

update rugen_result_sale_order set processor = '麦袜' where order_no = 'SO007036484';
update rugen_result_sale_order set processor = '麦正' where order_no = 'SO006993148';
update rugen_result_sale_order set processor = '麦袜' where order_no = 'SO007115959';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007250244';
update rugen_result_sale_order set processor = '麦袜' where order_no = 'SO007126829';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007236291';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007176123';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007122497';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007237463';
update rugen_result_sale_order set processor = '麦正' where order_no = 'SO007180099';
update rugen_result_sale_order set processor = '麦正' where order_no = 'SO006993148';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007192318';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007116505';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007236854';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007151160';
update rugen_result_sale_order set processor = '麦袜' where order_no = 'SO006978308';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007195515';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007161428';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006866092';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006680697';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006537453';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007201390';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006894616';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007223875';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007213701';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007062716';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007200866';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007225875';
update rugen_result_sale_order set processor = '麦斐3' where order_no = 'SO007167341';
update rugen_result_sale_order set processor = '麦斐3' where order_no = 'SO007163310';
update rugen_result_sale_order set processor = '麦斐3' where order_no = 'SO007167341';
update rugen_result_sale_order set processor = '麦袜' where order_no = 'SO007138712';
update rugen_result_sale_order set processor = '麦斐3' where order_no = 'SO006922028';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007252503';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006314024';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007141959';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007005160';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006973344';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007159972';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007027189';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007203448';
update rugen_result_sale_order set processor = '麦缇' where order_no = 'SO007178912';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006833898';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007208026';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007211459';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007122030';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007122873';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007206960';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007223592';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007034565';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006980883';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006947523';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006944710';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007005791';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007157125';
update rugen_result_sale_order set processor = '麦缇' where order_no = 'SO007168210';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007198867';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007176918';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007087301';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007220845';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007199621';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006850156';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007221106';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007221104';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007238385';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007240808';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006816555';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007011969';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007233209';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007220848';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO006842109';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007215904';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007234739';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007227535';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007095822';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007190788';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007164090';
update rugen_result_sale_order set processor = '麦缇' where order_no = 'SO007178848';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007199248';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007230915';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO006995455';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007234172';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007213349';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007176469';
update rugen_result_sale_order set processor = '麦浩' where order_no = 'SO007219463';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007236273';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007205991';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007133521';
update rugen_result_sale_order set processor = '麦朵' where order_no = 'SO007228220';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007189208';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007202433';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007186280';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007206103';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007227786';
update rugen_result_sale_order set processor = '星麦2' where order_no = 'SO007205987';




update rugen_result_sale_order set processor = '麦静1' where order_no =  'SO006689489';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO006685586';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO007023665';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO007007791';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007232459';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006962687';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO007178611';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006899639';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006965948';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006851283';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006962687';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO006915283';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006698065';
update rugen_result_sale_order set processor = '麦静1' where order_no =  'SO006689489';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO006685586';
update rugen_result_sale_order set processor = '麦静1' where order_no =  'SO006461773';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007220245';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007219938';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007079046';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007241580';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007225315';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007093763';
update rugen_result_sale_order set processor = '麦斐3' where order_no =  'SO007077668';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007219244';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007223127';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO007185389';
update rugen_result_sale_order set processor = '麦浩' where order_no =  'SO006875329';



update rugen_result_sale_order set processor='麦斐3' where  sales_channels like '%拼多多%' and sales_channels like '%尚玄%';
update rugen_result_sale_order set processor='麦斐3' where  sales_channels like '%抖店%' and sales_channels like '%尚玄%';

update rugen_result_sale_order set processor = '麦静1' where processor = '麦静' and  result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where processor = '麦斐3' and  result = '跟进中';
update rugen_result_sale_order set processor = '麦云1' where processor = '麦云' and  result = '跟进中';




delete from rugen_result_sale_order where order_no in (
'SO007605188', 
'SO007605191', 
'SO007605254', 
'SO007605605', 
'SO007605612', 
'SO007605744', 
'SO007605773', 
'SO007605805', 
'SO007605891', 
'SO007606001', 
'SO007606140', 
'SO007607129', 
'SO007607215', 
'SO007608658');


update  rugen_result_sale_order set processor = '麦腾' where profit >=-40 and hong_lv_deng ='红' and year(date_time)=2025 and month(date_time) <=3 ;


update rugen_result_sale_order set  processor = '星麦2'  where result = '跟进中' and sales_channels like '%抖店%' and sales_channels like '%公司%' and processor != '星麦2';

update rugen_result_sale_order set processor = '麦欣 'where sales_channels like '%速卖通%';

update rugen_result_sale_order set processor = '星麦2' where sales_channels = '抖店-公司户外鞋服专营店';



update rugen_result_sale_order set processor='麦巧'   where processor='星麦2' and after_sales_lable ='渠道重复扣款' and result = '跟进中';


update rugen_result_sale_order set processor = '麦啵3'   where processor='麦欣' and after_sales_lable ='退回渠道等待退款' and result = '跟进中' and sales_channels like '%速卖通%';
update rugen_result_sale_order set processor = '麦啵3'   where processor='麦欣' and after_sales_lable ='疑难订单' and result = '跟进中' and sales_channels like '%速卖通%';


update rugen_result_sale_order set processor='麦袜'   where  result = '跟进中' and pickingsource = 'TEMU';
update rugen_result_sale_order set processor='麦欣'   where  result = '跟进中' and sale_channel_id = 36;
update rugen_result_sale_order set processor='麦玺'   where  result = '跟进中' and pickingsource = 'TEMU' and 
(after_sales_lable ='紧急追款追件' or after_sales_lable ='快递丢件待赔付' or after_sales_lable ='疑难订单');



update rugen_result_sale_order set processor = '麦果'   where processor='麦欣' and sales_channels = '得物' and result = '跟进中';



update rugen_result_sale_order set processor = '麦斐3' where result = '跟进中' and sales_channels like '%京东%'  and sales_channels like '%尚玄%';
update rugen_result_sale_order set processor = '麦叮1' where result = '跟进中' and sales_channels like '%京东%'  and sales_channels like '%时禾美%';



update rugen_result_sale_order set  hong_lv_deng = '绿'  where  result = '跟进中' and pickingsource = 'TEMU' and processor='麦袜';

update rugen_result_sale_order  set processor = '麦斐3' where source = '4351202065780476537';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4306533013522055413';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4312749817839489937';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4299547212235728926';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2531338176096669185';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2569899144179580961';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2434924309843997167';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4288624490954846840';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4286756268721315641';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4303791614442553101';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4339666119091874128';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4294285311826701517';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2524501416946813272';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2580037034119351184';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2499027098870532661';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2511468589123038687';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4317614749745294431';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2536840452420709457';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4337969798419234634';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2548982064499244474';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2582067543463181991';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2582179248255881274';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2522830297735411372';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2430182427302088355';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4339283184448796135';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4261763774582717725';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4363066729469687420';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2503794937454523594';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4268121375097527444';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4241817830718757127';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2582969880404440190';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2573432294875921855';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4341470940182034220';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4324671578932369529';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2519271445944139578';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2551793559927180396';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2502958335537263958';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2593419204223627755';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4340948726113420837';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4347500006175765337';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4333088197157393547';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2575821576896643076';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4277227863358414710';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4288625390370432636';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2583505956786554381';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4188771219535970103';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4293880021612735832';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2545086650635681954';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4341127213787306234';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4373938441615178922';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4328548956583995414';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2540228631410426385';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2528558979438691280';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2556675373102176398';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4286538433112282512';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4279358376233575027';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4358288342871125615';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2596454510476905077';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4306830264776243707';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2538471649502596594';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4292240618741314729';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4270704516265350642';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4251498480947501019';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2559023040427718665';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4322083644192558936';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4358868336134328345';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4236310442901402628';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4326161688484474317';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2551964088715183987';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2491714093116926952';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4340436411652391508';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4379213271594383701';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2598045711525817360';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4375036765067934507';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2574576374719859775';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4284643899317481233';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2498534905056008496';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4275234003822295200';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2520321134531234794';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4299185307604827802';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4217973408751689043';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2578477658171832455';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2548459419098773750';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2514863316943651563';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2509015011647926693';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2612838432292015655';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2513500935824641273';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4341761280830793400';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4295611585600218532';
update rugen_result_sale_order  set processor = '麦斐3' where source = '2523712189047352362';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4321422829987163728';
update rugen_result_sale_order  set processor = '麦斐3' where source = '4342559149455855034';

update rugen_result_sale_order  set processor = '麦宋' where order_no = 'SO008606239';
update rugen_result_sale_order  set processor = '麦宋' where order_no = 'SO008862122';
update rugen_result_sale_order  set processor = '麦缇' where order_no = 'SO009129039';
update rugen_result_sale_order  set processor = '麦宋' where order_no = 'SO009073569';



update rugen_result_sale_order  set processor = '麦宋' where source = '4690169966444637740';
update rugen_result_sale_order  set processor = '麦宋' where source = '4815747720934261622';
update rugen_result_sale_order  set processor = '麦宋' where source = '2982952992560491176';
update rugen_result_sale_order  set processor = '麦宋' where source = '4792628557027683632';
update rugen_result_sale_order  set processor = '麦宋' where source = '2907470895608756252';


update rugen_result_sale_order  set processor = '麦浩' where order_no = 'SO009155605';
update rugen_result_sale_order  set processor = '麦浩' where order_no = 'SO009285651';
update rugen_result_sale_order  set processor = '麦浩' where source = '3074207811053058251';


update rugen_result_sale_order set processor = '麦静1' where processor = '麦静' and  result = '跟进中';
update rugen_result_sale_order set processor = '麦斐3' where processor = '麦斐3' and  result = '跟进中';
update rugen_result_sale_order set processor = '麦云1' where processor = '麦云' and  result = '跟进中';

   
update rugen_result_sale_order set processor = '麦赞3' where result = '跟进中' and sales_channels like '%得物%'  ;
update rugen_result_sale_order set processor = '麦果' where result = '跟进中' and sales_channels like '%得物%' and  after_sales_lable ='等待买家确认收货';
update rugen_result_sale_order set processor = '麦果' where result = '跟进中' and sales_channels like '%得物%' and  after_sales_lable ='订单售后中';

update rugen_result_sale_order set processor='麦正' where after_sales_lable ='已标记本地退待本地仓入库' and result = '跟进中' and processor = '麦袜';



-- update rugen_result_sale_order set processor='麦涛' where after_sales_lable ='已标记本地退待本地仓入库' and result = '跟进中';

update  rugen_result_sale_order set processor = '麦斐3' where sales_channels like '%鼎振运动%' and result = '跟进中' and date_time > '2026-03-06';
update  rugen_result_sale_order set processor = '麦斐3' where sales_channels like '%裕方运动%' and result = '跟进中' and date_time > '2026-03-06';


update  rugen_result_sale_order set processor = '麦叮1' where sales_channels = '抖店-方清裕体育专营店' and result = '跟进中' ;
update  rugen_result_sale_order set processor = '麦叮1' where sales_channels = '抖店-方清裕轻运动专营店' and result = '跟进中' ;

update  rugen_result_sale_order set processor = '星麦2' where sales_channels = '抖店-方清裕运动户外专营店' and result = '跟进中' ;
update  rugen_result_sale_order set processor = '星麦2' where sales_channels = '抖店-方清裕户外鞋服专营店' and result = '跟进中' ;
update  rugen_result_sale_order set processor = '星麦2' where sales_channels = '抖店-方清裕运动专营店' and result = '跟进中' ;


update  rugen_result_sale_order set processor = '麦叮1' where sales_channels like '%瑞方贝%' and result = '跟进中' ;

update  rugen_result_sale_order set processor = '麦朵' where sales_channels like '%聚倍%' and result = '跟进中' ;
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-聚倍轻运动专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-聚倍运动专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦静' where sales_channels = '抖店-聚倍运动户外专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';


update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店-聚倍体育专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';
update rugen_result_sale_order set processor = '麦浩' where sales_channels = '抖店-聚倍户外鞋服专营店' and (secondary_classification='未发秒退异常' or secondary_classification = '渠道重复扣款') and result = '跟进中';


update rugen_result_sale_order set processor = '麦叮1' where sales_channels = '抖店-尚玄户外鞋服专营店';






update rugen_result_sale_order 
set processor = '麦涛' 
where sales_channels like '内部出库%' and result = '跟进中';

delete from rugen_result_sale_order where sales_channels like '内部出库-员工%' and result = '跟进中';

delete from rugen_result_sale_order where sales_channels like '内部出库-公司自穿%' and result = '跟进中';


delete from rugen_result_sale_order where sales_channels like '内部出库%' and result = '跟进中' and pickingsource = 'TEMU';


"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "综合天猫按照标签店铺分配处理人 成功">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 综合天猫按照标签店铺分配处理人  失败   ">> ${errologs}   
    echo "$day_id^$i^$re^$?^$BEGIN_DATE^$END_DATE">> ${errologs}
fi









BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`

##  将数据moss库 中 下单日志表  订单日志表  退款成功订单 同步     并且将订单号 或者下单号 转化为  基础单号


mysql -hMYSQL_HOST -uUSER -PPORT -pPASS -D rugen -N -e "use rugen;select '' source,订单编号 from 订单日志 where 订单状态 = '退款成功';" >/data/exchange/订单日志_退款成功.txt



mysql -hRDS_HOST -ubig_data -pPASS -D big_data --local-infile -Bse "truncate table  订单日志_退款成功;load data local infile '/data/exchange/订单日志_退款成功.txt' into table 订单日志_退款成功 character set utf8mb4 fields terminated by '\t' lines terminated by '\n';

update 订单日志_退款成功 a,result_sale_order01 b set a.source=b.source where a.order_no=b.ps_gx_order_no;
delete from 订单日志_退款成功 where source = '';
"




## 同业平台退款成功订单 不参与未回款统计
mysql -hRDS_HOST -ubig_data -pPASS -D big_data -e "
delete  from rugen_result_sale_order where source in ( select distinct source from 订单日志_退款成功) and  result = '跟进中' and  date_time >= '2026-01-01 00:00:00' ;

"

if [ $? -eq 0 ]
then
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${logs}
    echo "同业数据更新 成功">> ${logs}
    echo "$day_id^^0^$BEGIN_DATE^$END_DATE">> ${logs}
else
    BEGIN_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
    echo "---------------" >> ${errologs}
    echo " 同业数据更新  失败   ">> ${errologs}   
    echo "$day_id^$i^$re^$?^$BEGIN_DATE^$END_DATE">> ${errologs}
fi









echo "---------------" >> ${logs}
endTime=`date '+%Y-%m-%d %H:%M:%S'`
endTime_s=`date +%s`
# 计算时长
sumTime=$[ $endTime_s - $startTime_s ]
echo "$startTime ---> $endTime" "Total:$sumTime seconds" >> ${logs}
echo "#######仓库货值情况 大类     指标计算完成">> ${logs}








