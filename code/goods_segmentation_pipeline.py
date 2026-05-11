import re
import pymysql
import jieba
from pymongo import MongoClient
from urllib.parse import quote_plus
from datetime import datetime, timedelta, date
import os
import time
import logging
import threading
import queue
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import multiprocessing
import gc
import csv
import uuid

# --- 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ================= 配置区 =================

MONGO_CONFIG = {
    'host': "MONGO_CLB_HOST",
    'port': 8877,
    'auth_db': "admin",
    'user': 'USER',
    'password': 'PASS',
    'db_name': "全店商品"
}

# MYSQL_CONFIG = {
#     'host': 'IP_ADDR',
#     'port': 3306,
#     'user': 'root',
#     'password': 'PASS',
#     'database': 'maiyuan',
#     'charset': 'utf8mb4',
#     'autocommit': False
# }
MYSQL_CONFIG = {
    'host': 'IP_ADDR',
    'port': 3306,
    'user': 'root',
    'password': 'PASS',
    'database': 'maiyuan',
    'charset': 'utf8mb4',
    'autocommit': False,
    'local_infile': True
}

DICT_FILE = r"INPUT_DICT.txt"

DAYS_TO_PROCESS = 30  
BATCH_SIZE = 1000     
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1) 

# ================= 全局辅助函数 =================

def extract_art_no(title):
    if not title: return ""
    pattern = r'[A-Za-z0-9-]{4,15}'
    potential_matches = re.findall(pattern, str(title))
    valid_matches = [item for item in potential_matches if any(c.isdigit() for c in item) ]
    # and any(c.isalpha() for c in item)
    return valid_matches[-1] if valid_matches else ""

def clean_numeric_value(value):
    if value is None: return None
    if isinstance(value, (int, float)): return value
    try:
        s = str(value).strip().replace(',', '').replace('，', '')
        if s in ('', '-', '--', 'null', 'None'): return None
        return float(s)
    except: return None

# ================= 子进程工作逻辑 =================

worker_jieba_initialized = False
worker_stopwords = set()
worker_patterns = {}
worker_dict_words = set()

def worker_init(dict_file, del_words=None):
    global worker_jieba_initialized, worker_stopwords, worker_patterns, worker_dict_words
    worker_patterns = {
        'cn_en': re.compile(r'([\u4e00-\u9fa5])([a-zA-Z0-9])'),
        'en_cn': re.compile(r'([a-zA-Z0-9])([\u4e00-\u9fa5])'),
        'en_space': re.compile(r'(?<=[a-zA-Z0-9.])\s+(?=[a-zA-Z0-9.])'),
        'pure_en': re.compile(r'^[a-zA-Z0-9_.-]+$'),
        'product_code': re.compile(r'(?=.*\d)([A-Za-z0-9]{4,12}(-[A-Za-z0-9]{2,4})?)\s*')
    }
    jieba.re_eng = re.compile(r"[a-zA-Z0-9_']+", re.U)
    if not worker_jieba_initialized:
        try:
            # 处理错词逻辑：调用 del_word
            if del_words:
                for word in del_words:
                    jieba.del_word(word)

            if Path(dict_file).exists():
                jieba.load_userdict(dict_file)
                with open(dict_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split()
                        if parts: worker_dict_words.add(parts[0])
            worker_jieba_initialized = True
        except: pass

def worker_process_batch(mongo_docs, process_time):
    global worker_stopwords, worker_patterns, worker_dict_words
    batch_seg, batch_unk = [], []
    numeric_keys = {'下单买家数', '下单件数', '商品加购人数', '商品收藏人数', '商品浏览量', '商品访客数', '平均停留时长', '成功退款金额', '搜索引导支付买家数', '搜索引导访客数', '支付买家数', '支付件数', '支付新买家数', '支付老卖家数', '支付金额', '访客平均价值', '年累计支付金额'}

    for doc in mongo_docs:
        try:
            goods_name = doc.get("商品名称", "")
            goods_id = str(doc.get("商品ID", ""))
            shop_name = doc.get("店铺名称", "")
            db_date = doc.get("数据库日期", "")
            if db_date and isinstance(db_date, str): db_date = db_date.replace('_', '-')
            art_no = extract_art_no(goods_name)

            def get_clean(key):
                val = doc.get(key)
                return clean_numeric_value(val) if key in numeric_keys else (str(val) if val is not None else "")

            base_data = (goods_id, goods_name, art_no, get_clean('下单买家数'), shop_name, get_clean('下单件数'), get_clean('下单转化率'), get_clean('商品加购人数'), get_clean('商品支付转化率'), get_clean('商品收藏人数'), get_clean('商品浏览量'), get_clean('商品访客数'), get_clean('商品详情页跳出率'), get_clean('平均停留时长'), get_clean('成功退款金额'), get_clean('搜索引导支付买家数'), get_clean('搜索引导支付转化率'), get_clean('搜索引导访客数'), get_clean('支付买家数'), get_clean('支付件数'), get_clean('支付新买家数'), get_clean('支付老卖家数'), get_clean('支付金额'), get_clean('访客平均价值'), db_date, get_clean('年累计支付金额'))

            text = worker_patterns['cn_en'].sub(r'\1 \2', str(goods_name))
            text = worker_patterns['en_cn'].sub(r'\1 \2', text)
            text = worker_patterns['en_space'].sub(r'_', text)
            
            words = jieba.lcut(text)
            merged_words, temp_english = [], []
            for word in words:
                if worker_patterns['pure_en'].match(word): temp_english.append(word.lower())
                else:
                    if temp_english: merged_words.append(''.join(temp_english)); temp_english = []
                    if word.strip(): merged_words.append(word.strip())
            if temp_english: merged_words.append(''.join(temp_english))
            
            valid_words = [w for w in merged_words if len(w) > 1 and w not in worker_stopwords and not w.isdigit()]
            for word in valid_words:
                if word not in worker_dict_words and not worker_patterns['product_code'].match(word):
                    batch_unk.append((word, goods_name, process_time))
                batch_seg.append((word,) + base_data + (process_time,))
        except: continue
    return batch_seg, batch_unk, len(mongo_docs)

# ================= 主流程控制器 =================

class DataPipeline:
    def __init__(self):
        self.total_saved = 0
        self.total_skipped = 0
        self.total_mongo_docs = 0
        self.processed_mongo_docs = 0
        self.is_running = True
        # 增大队列缓存，充当“中间数据”缓冲区 (5000个批次 * 1000文档/批 = 500万文档缓存)
        self.write_queue = queue.Queue(maxsize=5000)
        self.stats_lock = threading.Lock()
        self.start_time = None

    def get_mongo_connection(self):
        uri = f"mongodb://USER:PASS@MONGO_HOST/{MONGO_CONFIG['auth_db']}"
        return MongoClient(uri, serverSelectionTimeoutMS=5000)

    def init_mysql_table(self):
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                table_name = "goods_segmentation_30_days"
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    create_sql = f"""
                        CREATE TABLE {table_name} (
                            id INT AUTO_INCREMENT,
                            分词 VARCHAR(255), 商品ID VARCHAR(100), 商品名称 TEXT, 货号 VARCHAR(100), 
                            下单买家数 DOUBLE, 店铺名称 VARCHAR(255), 下单件数 DOUBLE, 下单转化率 VARCHAR(50), 
                            商品加购人数 DOUBLE, 商品支付转化率 VARCHAR(50), 商品收藏人数 DOUBLE, 商品浏览量 DOUBLE, 
                            商品访客数 DOUBLE, 商品详情页跳出率 VARCHAR(50), 平均停留时长 DOUBLE, 成功退款金额 DOUBLE, 
                            搜索引导支付买家数 DOUBLE, 搜索引导支付转化率 VARCHAR(50), 搜索引导访客数 DOUBLE, 
                            支付买家数 DOUBLE, 支付件数 DOUBLE, 支付新买家数 DOUBLE, 支付老卖家数 DOUBLE, 
                            支付金额 DOUBLE, 访客平均价值 DOUBLE, 数据库日期 DATE NOT NULL, 年累计支付金额 DOUBLE, 处理时间 DATETIME,
                            PRIMARY KEY (id, 数据库日期),
                            UNIQUE KEY idx_unique_segment (商品ID, 店铺名称, 数据库日期, 分词),
                            INDEX idx_word (分词), INDEX idx_goods_id (商品ID), INDEX idx_art_no (货号)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
                        PARTITION BY RANGE COLUMNS(数据库日期) (PARTITION p_init VALUES LESS THAN ('2023-01-01'))
                    """
                    cursor.execute(create_sql)
                    logger.info("初始分区表创建完成")
                else:
                    logger.info(f"表 {table_name} 已存在")
                
                # 检查并添加唯一索引（如果缺失）
                # 注意：如果表中已有重复数据，添加索引会失败。这里尝试添加，若失败则提示用户。
                cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = 'idx_unique_segment'")
                if not cursor.fetchone():
                    logger.warning(f"检测到 {table_name} 缺少唯一索引 idx_unique_segment，正在尝试添加...")
                    try:
                        # 使用 ALTER IGNORE 在旧版MySQL可去重，但新版不支持。
                        # 这里直接 ALTER，如果有重复会报错。
                        cursor.execute(f"ALTER TABLE {table_name} ADD UNIQUE KEY idx_unique_segment (商品ID, 店铺名称, 数据库日期, 分词)")
                        logger.info("成功添加唯一索引 idx_unique_segment")
                    except Exception as e:
                        logger.error(f"添加唯一索引失败（表中可能已有重复数据，请手动清理后重试）: {e}")

                cursor.execute("CREATE TABLE IF NOT EXISTS unknown_words (id INT AUTO_INCREMENT PRIMARY KEY, word VARCHAR(255), goods_name TEXT, process_time DATETIME, INDEX idx_word (word)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
            conn.commit()
        finally: conn.close()

    def get_latest_db_date(self):
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(数据库日期) FROM goods_segmentation_30_days")
                res = cursor.fetchone()
                if res and res[0]:
                    return res[0] if isinstance(res[0], date) else res[0].date()
                return None
        finally: conn.close()

    def manage_partitions(self, target_dates):
        
        if not target_dates: return
        
        # 1. 物理分区必须按日期升序添加
        sorted_dates = sorted([d.date() if isinstance(d, datetime) else d for d in target_dates])
        
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                table_name = "goods_segmentation_30_days"
                cursor.execute(f"SELECT PARTITION_NAME, PARTITION_DESCRIPTION FROM information_schema.partitions WHERE TABLE_SCHEMA = '{MYSQL_CONFIG['database']}' AND TABLE_NAME = '{table_name}'")
                p_info = cursor.fetchall()
                existing_names = {row[0] for row in p_info if row[0]}
                
                # 获取当前表最大的分区边界日期
                max_bound_date = None
                for name, desc in p_info:
                    if desc and '-' in desc:
                        d = datetime.strptime(desc.strip("'"), '%Y-%m-%d').date()
                        if max_bound_date is None or d > max_bound_date:
                            max_bound_date = d

                # 2. 升序尝试创建分区
                for d_obj in sorted_dates:
                    p_name = f"p{d_obj.strftime('%Y%m%d')}"
                    next_day_obj = d_obj + timedelta(days=1)
                    p_desc = next_day_obj.strftime('%Y-%m-%d')
                    
                    if p_name in existing_names: continue
                    
                    # 只有比当前最大边界更晚的分区才能直接 ADD PARTITION
                    if max_bound_date is None or next_day_obj > max_bound_date:
                        try:
                            cursor.execute(f"ALTER TABLE {table_name} ADD PARTITION (PARTITION {p_name} VALUES LESS THAN ('{p_desc}'))")
                            logger.info(f"成功添加新分区: {p_name}")
                            max_bound_date = next_day_obj 
                        except Exception as e:
                            logger.error(f"无法添加分区 {p_name}: {e}")
                    else:
                        logger.warning(f"跳过分区 {p_name}: 其边界 {p_desc} 早于或等于表内现有最大边界 {max_bound_date}")

                # 3. 自动清理过期分区 (保留 40 天)
                cutoff = date.today() - timedelta(days=40)
                for name, desc in p_info:
                    if not name or name == 'p_init': continue
                    try:
                        bound = datetime.strptime(desc.strip("'"), '%Y-%m-%d').date()
                        if bound < cutoff:
                            cursor.execute(f"ALTER TABLE {table_name} DROP PARTITION {name}")
                            logger.info(f"清理历史分区: {name}")
                    except: pass
            conn.commit()
        finally: conn.close()

    def monitor_worker(self):
        """监控线程：打印进度、速度和剩余时间"""
        while self.is_running:
            time.sleep(5)
            if not self.start_time or self.total_mongo_docs == 0:
                continue
            
            elapsed = time.time() - self.start_time
            if elapsed <= 0: continue
            
            with self.stats_lock:
                processed = self.processed_mongo_docs
                saved = self.total_saved
                skipped = self.total_skipped
            
            # 计算速度 (docs/s)
            speed_docs = processed / elapsed
            
            # 计算进度
            progress = (processed / self.total_mongo_docs) * 100
            
            # 计算剩余时间
            remaining_docs = self.total_mongo_docs - processed
            eta_seconds = remaining_docs / speed_docs if speed_docs > 0 else 0
            eta_str = str(timedelta(seconds=int(eta_seconds)))
            
            # 队列堆积情况
            q_size = self.write_queue.qsize()
            
            logger.info(f"进度: {progress:.2f}% | 已处理源文档: {processed}/{self.total_mongo_docs} | "
                        f"入库成功: {saved} | 重复跳过: {skipped} | 待写批次: {q_size} | "
                        f"速度: {speed_docs:.1f} docs/s | 预计剩余: {eta_str}")

    def writer_worker(self, worker_id):
        conn = None
        try:
            # 必须开启 local_infile=True
            conn = pymysql.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            
            cols = ['分词', '商品ID', '商品名称', '货号', '下单买家数', '店铺名称', '下单件数', '下单转化率', '商品加购人数', '商品支付转化率', '商品收藏人数', '商品浏览量', '商品访客数', '商品详情页跳出率', '平均停留时长', '成功退款金额', '搜索引导支付买家数', '搜索引导支付转化率', '搜索引导访客数', '支付买家数', '支付件数', '支付新买家数', '支付老卖家数', '支付金额', '访客平均价值', '数据库日期', '年累计支付金额', '处理时间']
            
            # 缓冲区
            seg_buffer = []
            unk_buffer = []
            
            # 动态调整配置
            base_threshold = 1000
            max_threshold = 10000
            current_flush_threshold = base_threshold
            
            # 性能指标
            metrics = {
                'sort_time': 0.0,
                'retry_count': 0,
                'flush_count': 0
            }

            def flush_data(data_type, buffer_list):
                if not buffer_list: return
                
                start_flush_time = time.time()
                
                # 2. 数据预排序优化 (仅针对 segmentation 数据)
                # 唯一索引: (商品ID, 店铺名称, 数据库日期, 分词)
                # 对应 cols 索引: 商品ID(1), 店铺名称(5), 数据库日期(25), 分词(0)
                if data_type == 'segmentation':
                    sort_start = time.time()
                    try:
                        # 确保排序键值为字符串，避免 None 比较错误
                        buffer_list.sort(key=lambda x: (
                            str(x[1]) if x[1] is not None else "", 
                            str(x[5]) if x[5] is not None else "", 
                            str(x[25]) if x[25] is not None else "", 
                            str(x[0]) if x[0] is not None else ""
                        ))
                        metrics['sort_time'] += (time.time() - sort_start)
                    except Exception as e:
                        logger.error(f"[Writer-{worker_id}] 排序失败: {e}")

                # 生成临时 CSV 文件
                temp_filename = f"temp_{data_type}_{worker_id}_{uuid.uuid4()}.csv"
                abs_path = os.path.abspath(temp_filename).replace('\\', '/')
                
                try:
                    with open(temp_filename, 'w', newline='', encoding='utf-8') as f:
                        # 使用 csv.writer 处理转义和引用，遇到 None 转为 \N (MySQL NULL)
                        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        for row in buffer_list:
                            # 将 None 转换为 r'\N' 以便 MySQL 识别为 NULL
                            # 注意：如果是字符串 'None' 还是 'None'，只有真正的 None 变成 \N
                            cleaned_row = [r'\N' if x is None else x for x in row]
                            writer.writerow(cleaned_row)
                    
                    # LOAD DATA LOCAL INFILE
                    table_name = "goods_segmentation_30_days" if data_type == 'segmentation' else "unknown_words"
                    target_cols = cols if data_type == 'segmentation' else ['word', 'goods_name', 'process_time']
                    
                    # IGNORE 跳过重复键
                    sql = f"""
                        LOAD DATA LOCAL INFILE '{abs_path}' 
                        IGNORE INTO TABLE {table_name} 
                        CHARACTER SET utf8mb4 
                        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' 
                        LINES TERMINATED BY '\r\n' 
                        ({', '.join([f'`{c}`' for c in target_cols])})
                    """
                    
                    # 1. 死锁重试机制
                    max_retries = 5
                    for attempt in range(max_retries + 1):
                        try:
                            cursor.execute(sql)
                            conn.commit()
                            
                            # 统计行数
                            if data_type == 'segmentation':
                                # LOAD DATA 返回的信息通常包含 Records: X  Deleted: Y  Skipped: Z  Warnings: W
                                # cursor.rowcount 在 pymysql 中对于 LOAD DATA 通常返回影响行数
                                count = cursor.rowcount
                                
                                # 解析 info 获取跳过数量
                                skipped = 0
                                info = getattr(conn, 'info', None)
                                if info:
                                    # info 格式示例: Records: 100  Deleted: 0  Skipped: 0  Warnings: 0
                                    match = re.search(r'Skipped:\s*(\d+)', str(info))
                                    if match:
                                        skipped = int(match.group(1))
                                        
                                with self.stats_lock: 
                                    self.total_saved += count
                                    self.total_skipped += skipped
                            
                            metrics['flush_count'] += 1
                            break # 成功则退出重试循环
                            
                        except pymysql.err.OperationalError as e:
                            # 错误码 1213: Deadlock found
                            if e.args[0] == 1213:
                                if attempt < max_retries:
                                    metrics['retry_count'] += 1
                                    sleep_time = 1 + random.uniform(0, 2) # 1-3秒随机抖动
                                    logger.warning(f"[Writer-{worker_id}] 遭遇死锁 (尝试 {attempt+1}/{max_retries})，休眠 {sleep_time:.2f}s 后重试...")
                                    time.sleep(sleep_time)
                                    continue
                                else:
                                    logger.error(f"[Writer-{worker_id}] 死锁重试次数耗尽，放弃本批次写入: {e}")
                                    conn.rollback()
                                    raise
                            else:
                                raise # 其他 OperationalError 直接抛出
                        except Exception as e:
                            conn.rollback()
                            raise # 其他异常直接抛出
                        
                except Exception as e:
                    logger.error(f"[Writer-{worker_id}] LOAD DATA 失败: {e}")
                    conn.rollback()
                finally:
                    # 清理临时文件
                    if os.path.exists(temp_filename):
                        try:
                            os.remove(temp_filename)
                        except: pass
                    buffer_list.clear()

            while True:
                try:
                    # 3. 动态批次大小调整
                    q_depth = self.write_queue.qsize()
                    if q_depth > 4000: # 高负载
                        current_flush_threshold = 10000 # 峰值
                    elif q_depth > 2000:
                        current_flush_threshold = 5000 # 高负载
                    else:
                        current_flush_threshold = 1000 # 正常
                        
                    item = self.write_queue.get(timeout=3)
                    if item is None: 
                        # 收到结束信号，处理剩余数据后退出
                        flush_data('segmentation', seg_buffer)
                        flush_data('unknown', unk_buffer)
                        logger.info(f"[Writer-{worker_id}] 统计: SortTime={metrics['sort_time']:.2f}s, Retries={metrics['retry_count']}, Flushes={metrics['flush_count']}")
                        self.write_queue.put(None)
                        break
                    
                    tp, data = item
                    
                    if tp == 'segmentation':
                        seg_buffer.extend(data)
                        if len(seg_buffer) >= current_flush_threshold:
                            flush_data('segmentation', seg_buffer)
                    else:
                        unk_buffer.extend(data)
                        if len(unk_buffer) >= current_flush_threshold:
                            flush_data('unknown', unk_buffer)
                            
                except queue.Empty:
                    if not self.is_running: break
                    # 空闲时也可以 flush
                    flush_data('segmentation', seg_buffer)
                    flush_data('unknown', unk_buffer)
                except Exception as e:
                    logger.error(f"[Writer-{worker_id}] 数据处理异常: {e}")
                finally: 
                    try:
                        self.write_queue.task_done()
                    except ValueError: pass
        finally:
            if conn: conn.close()
            logger.info(f"[Writer-{worker_id}] 线程退出")

    def run_daily_refresh(self):
        """调用存储过程 Daily_segment_refresh"""
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                logger.info("正在调用存储过程 Daily_segment_refresh，请等待执行完成...")
                start_t = time.time()
                cursor.execute("CALL Daily_segment_refresh()")
                conn.commit()
                elapsed = time.time() - start_t
                logger.info(f"存储过程 Daily_segment_refresh 执行成功，耗时: {elapsed:.2f}s")
        except Exception as e:
            logger.error(f"存储过程 Daily_segment_refresh 执行失败: {e}")
        finally:
            conn.close()

    def run(self):
        self.start_time = time.time()
        self.init_mysql_table()
        
        # 恢复为单线程写入，彻底避免死锁
        # 通过增大 Queue 缓存和 batch size 来保证速度
        num_writers = 2
        writer_threads = []
        for i in range(num_writers):
            t = threading.Thread(target=self.writer_worker, args=(i,), daemon=True)
            t.start()
            writer_threads.append(t)
            
        threading.Thread(target=self.monitor_worker, daemon=True).start()
        
        try:
            client = self.get_mongo_connection()
            db = client[MONGO_CONFIG['db_name']]
            
            today = date.today()
            last_date = self.get_latest_db_date()
            
            # 增量筛选：从上次结束日期到今天
            if last_date:
                diff = (today - last_date).days
                target_dates = [last_date + timedelta(days=i) for i in range(max(0, diff + 1))]
            else:
                target_dates = [today - timedelta(days=i) for i in range(1, DAYS_TO_PROCESS + 1)]
            
            target_dates = sorted(list(set(target_dates)), reverse=True) # 跑数据建议从新到旧
            self.manage_partitions(target_dates) # 创建分区必须包含这些日期

            # 计算总任务量
            logger.info("正在计算预计任务总量...")
            for d in target_dates:
                col_name = f"全店商品_{d.strftime('%Y_%m_%d')}"
                if col_name in db.list_collection_names():
                    self.total_mongo_docs += db[col_name].estimated_document_count()
            logger.info(f"预计处理总文档数: {self.total_mongo_docs}")
            
            # 读取错词列表（原属性词逻辑修改）
            del_words = []
            attr_file = r"D:\project\停用词20260207091346.xlsx"
            if os.path.exists(attr_file):
                try:
                    import pandas as pd
                    df = pd.read_excel(attr_file)
                    # 读取第一列的数据
                    if not df.empty:
                        first_col_name = df.columns[0]
                        del_words = df[first_col_name].dropna().astype(str).tolist()
                    logger.info(f"加载错词列表完成，共 {len(del_words)} 个")
                except Exception as e:
                    logger.error(f"加载错词列表失败: {e}")
            else:
                logger.warning(f"错词文件不存在: {attr_file}")

            proc_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pool = ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=worker_init, initargs=(DICT_FILE, del_words))
            
            futures = []
            for d in target_dates:
                col_name = f"全店商品_{d.strftime('%Y_%m_%d')}"
                if col_name not in db.list_collection_names(): continue
                
                logger.info(f"开始同步 MongoDB 集合: {col_name}")
                # 使用游标而非直接转 list 以省内存
                cursor = db[col_name].find({})
                batch_docs = []
                for doc in cursor:
                    batch_docs.append(doc)
                    if len(batch_docs) >= BATCH_SIZE:
                        futures.append(pool.submit(worker_process_batch, batch_docs, proc_time))
                        batch_docs = []
                        
                        # 流控：防止任务堆积导致内存溢出
                        # 检查完成的任务
                        done_indices = []
                        for i, f in enumerate(futures):
                            if f.done():
                                done_indices.append(i)
                        
                        # 处理已完成的任务
                        for i in sorted(done_indices, reverse=True):
                            f = futures.pop(i)
                            try:
                                res, unk, count = f.result()
                                with self.stats_lock:
                                    self.processed_mongo_docs += count
                                if res: self.write_queue.put(('segmentation', res))
                                if unk: self.write_queue.put(('unknown', unk))
                            except Exception as e:
                                logger.error(f"任务异常: {e}")

                        # 如果堆积太多，暂停提交
                        while len(futures) > MAX_WORKERS * 2:
                            time.sleep(0.5)
                            # 再次检查并清理
                            done_indices = []
                            for i, f in enumerate(futures):
                                if f.done():
                                    done_indices.append(i)
                            
                            for i in sorted(done_indices, reverse=True):
                                f = futures.pop(i)
                                try:
                                    res, unk, count = f.result()
                                    with self.stats_lock:
                                        self.processed_mongo_docs += count
                                    if res: self.write_queue.put(('segmentation', res))
                                    if unk: self.write_queue.put(('unknown', unk))
                                except Exception as e:
                                    logger.error(f"任务异常: {e}")
                
                if batch_docs:
                    futures.append(pool.submit(worker_process_batch, batch_docs, proc_time))

            # 等待所有任务完成
            for f in as_completed(futures):
                try:
                    res, unk, count = f.result()
                    with self.stats_lock:
                        self.processed_mongo_docs += count
                    if res: self.write_queue.put(('segmentation', res))
                    if unk: self.write_queue.put(('unknown', unk))
                except Exception as e:
                    logger.error(f"最终任务异常: {e}")

            # 发送结束信号
            self.write_queue.put(None)
            
            # 等待写入线程退出
            for t in writer_threads:
                t.join()
            
            elapsed = time.time() - self.start_time

            logger.info(f"=== 同步完成！共新增入库 {self.total_saved:,} 条记录 | 总耗时: {elapsed:.1f}s ===")

            # 执行每日刷新存储过程
            self.run_daily_refresh()
            
        finally:
            self.is_running = False
            client.close()
            # 确保进程池关闭
            if 'pool' in locals():
                pool.shutdown(wait=False)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    DataPipeline().run()