import threading
import queue
import time
import logging
import logging.handlers
import sys
import os
import traceback
from datetime import datetime
from urllib.parse import quote_plus

from pymongo import MongoClient
from bson import ObjectId
import json
from sqlalchemy import create_engine, text, Column, String, Text, inspect, Table, MetaData
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert as mysql_insert


class Config:
    # MongoDB 本地
    # MONGO_HOSTS = "IP_ADDR,IP_ADDR,IP_ADDR"
    # MONGO_DB = "视觉下图"
    # MONGO_COLLECTION = "下图日志"
    # MONGO_USER = 'USER'
    # MONGO_PASS = 'PASS'
    # MONGO_AUTH_DB = "admin"

    #MongoDB 阿里云
    MONGO_HOSTS = "MONGO_CLB_HOST:8877, MONGO_CLB_HOST:8878"
    MONGO_DB = "视觉下图"
    MONGO_COLLECTION = "下图日志"
    MONGO_USER = 'USER'
    MONGO_PASS = 'PASS'
    MONGO_AUTH_DB = "admin"

    # MySQL
    # MYSQL_HOST = "IP_ADDR"
    # MYSQL_PORT = 3306
    # MYSQL_USER = "root"
    # MYSQL_PASS = 'PASS'
    # MYSQL_DB = "视觉下图"
    # MYSQL_TABLE = "下图日志_bak"

    # testMySQL
    MYSQL_HOST = "IP_ADDR"
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASS = 'PASS'
    MYSQL_DB = "maiyuan"
    MYSQL_TABLE = "下图日志"

    BATCH_SIZE = 5000
    QUEUE_SIZE = 20000
    SYNC_INTERVAL_HOURS = 3
    
    MYSQL_POOL_SIZE = 20
    MYSQL_MAX_OVERFLOW = 30
    WRITER_THREADS = 5

    LOG_FILE = "下图日志sync_task.log"


def setup_logger():
    logger = logging.getLogger("SyncService")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    file_handler = logging.handlers.RotatingFileHandler(
        Config.LOG_FILE, 
        maxBytes=100*1024*1024, 
        backupCount=5, 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logger()

stop_event = threading.Event()


class MongoReader:
    """
    负责从 MongoDB 读取数据并放入队列。
    重构：不再继承 threading.Thread，而是在主线程中直接运行，避免线程反复创建。
    """
    def __init__(self, queue, last_id=None):
        self.queue = queue
        self.last_id = last_id
        self.client = None

    def connect(self):
        try:
            user = quote_plus(Config.MONGO_USER)
            password = quote_plus(Config.MONGO_PASS)
            uri = f"mongodb://USER:PASS@MONGO_HOST/{Config.MONGO_AUTH_DB}?authSource={Config.MONGO_AUTH_DB}"

            self.client = MongoClient(uri)
            self.client.admin.command('ping')
            logger.info("MongoDB 连接成功")
        except Exception as e:
            logger.error(f"MongoDB 连接失败: {e}")
            raise

    def read(self):
        """
        执行读取逻辑，将数据放入队列。此方法会阻塞直到本次批次读取完成。
        """
        try:
            self.connect()
            db = self.client[Config.MONGO_DB]
            collection = db[Config.MONGO_COLLECTION]

            query = {}
            if self.last_id:
                try:
                    query["_id"] = {"$gt": ObjectId(self.last_id)}
                    logger.info(f"增量同步启动，起始 ID (不包含): {self.last_id}")
                except Exception as e:
                    logger.error(f"转换 last_id 出错: {e}，将进行全量同步")
            else:
                logger.info("未找到断点，开始全量同步")

            cursor = collection.find(query).sort("_id", 1).batch_size(Config.BATCH_SIZE)

            count = 0
            for doc in cursor:
                if stop_event.is_set(): 
                    break

                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

                self.queue.put(doc)
                count += 1

                if count % 10000 == 0:
                    logger.info(f"已读取 {count} 条数据...")

            logger.info(f"MongoDB 读取完成，共读取 {count} 条数据")

        except Exception as e:
            logger.error(f"MongoReader 异常: {e}")
            traceback.print_exc()
        finally:
            if self.client:
                self.client.close()


class SharedSchemaManager:
    def __init__(self, engine):
        self.engine = engine
        self.known_columns = set()
        self.newly_added_columns = set()
        self.column_lock = threading.Lock()
        self._load_existing_columns()

    def _load_existing_columns(self):
        """Initialize known columns from database schema."""
        try:
            inspector = inspect(self.engine)
            if inspector.has_table(Config.MYSQL_TABLE):
                columns = [col['name'] for col in inspector.get_columns(Config.MYSQL_TABLE)]
                self.known_columns.update(columns)
                logger.info(f"已加载现有列: {self.known_columns}")
            else:
                logger.info("表尚不存在。")
        except Exception as e:
            logger.error(f"加载列失败: {e}")

    def ensure_columns(self, doc_keys):
        """Check and add missing columns dynamically. Returns set of new columns added."""
        if doc_keys.issubset(self.known_columns):
            return set()

        with self.column_lock:
            new_columns = doc_keys - self.known_columns
            if not new_columns:
                return set()

            added = set()
            with self.engine.connect() as conn:
                for col in new_columns:
                    try:
                        col_type = "TEXT"
                        alter_sql = text(f"ALTER TABLE `{Config.MYSQL_TABLE}` ADD COLUMN `{col}` {col_type}")
                        conn.execute(alter_sql)
                        self.known_columns.add(col)
                        self.newly_added_columns.add(col)
                        added.add(col)
                        logger.info(f"添加新列: {col} ({col_type})")
                        logger.info(f"提示: 检测到新列 '{col}'。同步后将自动回填。")
                    except Exception as e:
                        logger.error(f"添加列 {col} 失败: {e}")
            return added


global_insert_count = 0
global_start_time = time.time()
global_lock = threading.Lock()


class MySQLWriter(threading.Thread):
    def __init__(self, queue, engine, schema_manager):
        super().__init__(daemon=True)  # Daemon thread
        self.queue = queue
        self.engine = engine
        self.schema_manager = schema_manager
        self.success_count = 0
        self.fail_count = 0
        self.local_processed_count = 0  # 线程内累计处理数
        self.metadata = MetaData()
        self.table = None
        self._reload_table()

    def _reload_table(self):
        """Reload table definition from database using reflection."""
        try:
            self.metadata.clear()
            self.table = Table(Config.MYSQL_TABLE, self.metadata, autoload_with=self.engine)
        except Exception as e:
            logger.error(f"{self.name}: 重新加载表结构失败: {e}")

    def run(self):
        buffer = []
        logger.info(f"{self.name} 启动")
        last_heartbeat_time = time.time()

        try:
            while not stop_event.is_set():

                if time.time() - last_heartbeat_time > 1800:  
                    logger.info(f"{self.name} 存活检查 - 当前累计写入: {self.success_count}, 失败: {self.fail_count}")
                    last_heartbeat_time = time.time()

                try:

                    doc = self.queue.get(timeout=1)
                except queue.Empty:
 
                    if buffer:
                        self.flush_buffer(buffer)

                        for _ in range(len(buffer)):
                            self.queue.task_done()
                        buffer = []
                    continue

                try:
                    clean_doc = {}
                    for k, v in doc.items():
                        if k == '_id':
                            clean_doc[k] = str(v)
                        elif isinstance(v, (list, dict)):
                            clean_doc[k] = json.dumps(v, ensure_ascii=False)
                        else:
                            clean_doc[k] = v
                    
                    buffer.append(clean_doc)
                    self.local_processed_count += 1
                    

                    if self.local_processed_count % 5000 == 0:
                        logger.info(f"{self.name} 正在运行 - 已处理: {self.local_processed_count} 条")

                except Exception as e:
                    logger.error(f"数据处理错误: {e}, 数据: {doc.get('_id')}")
                    self.fail_count += 1

                    self.queue.task_done()

                if len(buffer) >= Config.BATCH_SIZE:
                    self.flush_buffer(buffer)
                    for _ in range(len(buffer)):
                        self.queue.task_done()
                    buffer = []

        except Exception as e:
            logger.error(f"{self.name} 异常: {e}")
            traceback.print_exc()
        finally:
            if buffer:
                logger.info(f"{self.name} :停止前刷盘剩余 {len(buffer)}条缓冲数据")
                self.flush_buffer(buffer)
                for _ in range(len(buffer)):
                    try:
                        self.queue.task_done()
                    except ValueError:
                        pass
            logger.info(f"{self.name} 停止")

    def flush_buffer(self, buffer):
        if not buffer:
            return
        
        try:
            batch_keys = set().union(*(d.keys() for d in buffer))
            

            new_cols = self.schema_manager.ensure_columns(batch_keys)
            
            if new_cols:
                logger.info(f"{self.name}: 检测到新列，正在重新加载表结构。")
                self._reload_table()

            normalized_buffer = []
            for item in buffer:
                normalized_item = {k: item.get(k, None) for k in batch_keys}
                normalized_buffer.append(normalized_item)


            start_time = time.time()
            
            stmt = mysql_insert(self.table).values(normalized_buffer)
            
            if normalized_buffer:
                update_cols = {col: stmt.inserted[col] for col in batch_keys if col != '_id'}
                on_duplicate_stmt = stmt.on_duplicate_key_update(update_cols)
            else:
                on_duplicate_stmt = stmt

            with self.engine.begin() as conn:
                conn.execute(on_duplicate_stmt)
            
            duration = time.time() - start_time
            count = len(normalized_buffer)
            self.success_count += count
            

            logger.info(f"{self.name} 批量写入成功: 本次 {count} 条, 累计 {self.success_count} 条 (耗时 {duration:.2f}s)")
            
            global global_insert_count
            with global_lock:
                global_insert_count += count
                current_total = global_insert_count
            
            if current_total % 100000 < Config.BATCH_SIZE * Config.WRITER_THREADS:
                total_duration = time.time() - global_start_time
                global_tps = current_total / total_duration if total_duration > 0 else 0
                logger.info(f"全局进度: {current_total} 行。平均 TPS: {global_tps:.2f}")

        except Exception as e:

            logger.warning(f"{self.name} 批量插入失败 (Buffer: {len(buffer)}条): {type(e).__name__}: {e}。正在检查表结构并重试...")

            try:
                 batch_keys = set().union(*(d.keys() for d in buffer))
                 self.schema_manager.ensure_columns(batch_keys)
                 
                 normalized_buffer = []
                 for item in buffer:
                    normalized_item = {k: item.get(k, None) for k in batch_keys}
                    normalized_buffer.append(normalized_item)

                 stmt = mysql_insert(self.table).values(normalized_buffer)
                 if normalized_buffer:
                     update_cols = {col: stmt.inserted[col] for col in batch_keys if col != '_id'}
                     on_duplicate_stmt = stmt.on_duplicate_key_update(update_cols)
                 else:
                     on_duplicate_stmt = stmt

                 with self.engine.begin() as conn:
                    conn.execute(on_duplicate_stmt)
                 self.success_count += len(buffer)
                 logger.info(f"{self.name} 重试批量插入成功 (本次 {len(buffer)} 条)。")
            except Exception as retry_e:
                logger.error(f"{self.name} 重试失败: {type(retry_e).__name__}。回退到逐行插入模式。")

                for item in buffer:
                    try:
                        keys = list(item.keys())
                        quoted_c = [f"`{k}`" for k in keys]
                        p_holders = [f":{k}" for k in keys]
                        
                        update_clause = ", ".join([f"`{k}`=VALUES(`{k}`)" for k in keys if k != '_id'])
                        single_sql = text(f"INSERT INTO `{Config.MYSQL_TABLE}` ({', '.join(quoted_c)}) VALUES ({', '.join(p_holders)}) ON DUPLICATE KEY UPDATE {update_clause}")

                        with self.engine.begin() as conn:
                            conn.execute(single_sql, item)
                        self.success_count += 1
                    except Exception as raw_e:
                        logger.error(f"单条写入失败 ID {item.get('_id')}: {type(raw_e).__name__} - {raw_e}")
                        self.fail_count += 1


class SyncService:
    def __init__(self):
        self.engine = None
        self.init_mysql()
        

        self.queue = queue.Queue(maxsize=Config.QUEUE_SIZE)
        self.writers = []
        self._start_writers()

    def init_mysql(self):
        try:
            conn_str = f"mysql+pymysql://{Config.MYSQL_USER}:{Config.MYSQL_PASS}@{Config.MYSQL_HOST}:{Config.MYSQL_PORT}/{Config.MYSQL_DB}?charset=utf8mb4"
            self.engine = create_engine(
                conn_str, 
                pool_recycle=3600,
                pool_size=Config.MYSQL_POOL_SIZE,
                max_overflow=Config.MYSQL_MAX_OVERFLOW
            )
            
            metadata = MetaData()
            Table(
                Config.MYSQL_TABLE, metadata,
                Column('_id', String(24), primary_key=True, comment="MongoDB ObjectId"),
                Column('图片链接', Text, comment="图片链接"),
                Column('md5', String(32), comment="MD5"),
                Column('货号', String(500), comment="货号"),
                Column('图片顺序', Text, comment="图片顺序"),
                Column('insert_time', String(50), comment="插入时间"),
                Column('来源', String(50), comment="来源"),
                Column('图片属性', String(50), comment="图片属性"),
                Column('color', String(50), comment="颜色"),
                Column('tag', String(50), comment="标签"),
                Column('AI去重', String(10), comment="AI去重")
            )
            metadata.create_all(self.engine)
            
            logger.info("MySQL 表结构检查/创建完成")
            self.schema_manager = SharedSchemaManager(self.engine)
            
            try:
                with self.engine.begin() as conn:
                    logger.info("正在配置 MySQL 会话以实现高性能...")
                    conn.execute(text("SET GLOBAL innodb_flush_log_at_trx_commit = 2"))
                    conn.execute(text("SET GLOBAL sync_binlog = 0"))
            except Exception as e:
                logger.warning(f"设置全局性能变量失败 (可能需要 SUPER 权限): {e}")

        except Exception as e:
            logger.critical(f"MySQL 初始化失败: {e}")
            raise

    def _start_writers(self):
        logger.info(f"正在启动 {Config.WRITER_THREADS} 个 MySQLWriter 线程...")
        for i in range(Config.WRITER_THREADS):
            w = MySQLWriter(self.queue, self.engine, self.schema_manager)
            w.name = f"MySQLWriter-{i+1}"
            w.start()
            self.writers.append(w)

    def get_last_synced_id(self):
        """获取 MySQL 中最大的 _id"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(_id) FROM {Config.MYSQL_TABLE}"))
                last_id = result.scalar()
                return last_id
        except Exception as e:
            logger.error(f"获取断点失败: {e}")
            return None

    def backfill_missing_fields(self, column_name):
        """
        Backfill missing fields for a specific column.
        Queries MySQL for records where column is NULL, fetches from MongoDB, and updates MySQL.
        """
        logger.info(f"开始回填列: {column_name}")
        try:
            user = quote_plus(Config.MONGO_USER)
            password = quote_plus(Config.MONGO_PASS)
            uri = f"mongodb://USER:PASS@MONGO_HOST/{Config.MONGO_AUTH_DB}?authSource={Config.MONGO_AUTH_DB}"
            mongo_client = MongoClient(uri)
            mongo_db = mongo_client[Config.MONGO_DB]
            mongo_coll = mongo_db[Config.MONGO_COLLECTION]

            last_id = ""
            total_updated = 0
            
            while not stop_event.is_set():
                with self.engine.connect() as conn:
                    sql = text(f"SELECT _id FROM `{Config.MYSQL_TABLE}` WHERE `{column_name}` IS NULL AND `_id` > :last_id ORDER BY `_id` ASC LIMIT 1000")
                    result = conn.execute(sql, {"last_id": last_id}).fetchall()
                
                if not result:
                    break
                
                ids_to_update = [row[0] for row in result]
                last_id = ids_to_update[-1]
                
                mongo_ids = []
                for mid in ids_to_update:
                    try:
                        oid = ObjectId(mid)
                        mongo_ids.append(oid)
                    except:
                        mongo_ids.append(mid)
                        
                cursor = mongo_coll.find({"_id": {"$in": mongo_ids}}, {"_id": 1, column_name: 1})
                
                updates = []
                for doc in cursor:
                    val = doc.get(column_name)
                    if val is not None:
                        if isinstance(val, (list, dict)):
                            val = json.dumps(val, ensure_ascii=False)
                        
                        updates.append({
                            "target_id": str(doc["_id"]),
                            "new_val": val
                        })
                
                if updates:
                    with self.engine.begin() as conn:
                        update_sql = text(f"UPDATE `{Config.MYSQL_TABLE}` SET `{column_name}` = :new_val WHERE `_id` = :target_id")
                        conn.execute(update_sql, updates)
                    total_updated += len(updates)
                    logger.info(f"回填列 '{column_name}': 已处理批次 {len(updates)} 条记录。目前总共更新: {total_updated}")
            
            mongo_client.close()
            logger.info(f"回填完成。列 {column_name} 共更新 {total_updated} 条记录。")

        except Exception as e:
            logger.error(f"回填失败: {e}")
            traceback.print_exc()

    def run_once(self):
        start_time = datetime.now()
        logger.info(f"=== 同步任务开始于 {start_time} ===")

        last_id = self.get_last_synced_id()


        reader = MongoReader(self.queue, last_id)
        reader.read()

        logger.info("MongoReader 完成读取，正在等待写入队列排空...")
        

        self.queue.join()
        
        logger.info("队列已排空，本批次写入完成")

        logger.info("=== 活跃线程状态快照 ===")
        for t in threading.enumerate():
            logger.info(f"线程: {t.name}, 状态: {'Alive' if t.is_alive() else 'Dead'}, Daemon: {t.daemon}")
        logger.info("========================")

        if self.schema_manager.newly_added_columns and not stop_event.is_set():
            logger.info(f"同步期间检测到新列: {self.schema_manager.newly_added_columns}。开始自动回填...")
            for col in self.schema_manager.newly_added_columns:
                try:
                    if stop_event.is_set(): break
                    self.backfill_missing_fields(col)
                except Exception as e:
                    logger.error(f"自动回填列 {col} 失败: {e}")

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== 同步任务结束于 {end_time}，耗时: {duration} ===")

    def start_loop(self):
        logger.info(f"启动增量同步服务，间隔 {Config.SYNC_INTERVAL_HOURS} 小时")
        while not stop_event.is_set():
            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("服务已停止")
                stop_event.set()
                break
            except Exception as e:
                logger.error(f"任务执行中发生未捕获异常: {e}")
                traceback.print_exc()

            if stop_event.is_set():
                break

            logger.info(f"等待 {Config.SYNC_INTERVAL_HOURS} 小时后进行下一次同步...")

            for _ in range(Config.SYNC_INTERVAL_HOURS * 3600):
                if stop_event.is_set():
                    break
                time.sleep(1)


if __name__ == "__main__":
    pid_file = "sync.pid"
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                old_pid = int(f.read().strip())
            
            is_running = False
            try:
                import psutil
                if psutil.pid_exists(old_pid):
                     is_running = True
            except ImportError:
                if sys.platform == 'win32':
                     output = os.popen(f'tasklist /FI "PID eq {old_pid}"').read()
                     if str(old_pid) in output:
                         is_running = True
                else:
                     try:
                         os.kill(old_pid, 0)
                         is_running = True
                     except OSError:
                         pass

            if is_running:
                print(f"程序已在运行 (PID: {old_pid})，请勿重复启动。")
                sys.exit(1)
            else:
                print("检测到残留的 PID 文件，但进程不存在，将覆盖。")
        except Exception:
            pass

    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))

    try:
        service = SyncService()
        service.start_loop()
    except KeyboardInterrupt:
        logger.info("服务已停止")
        stop_event.set()
    except Exception as e:
        logger.critical(f"服务启动失败: {e}")
        traceback.print_exc()
    finally:
        if os.path.exists(pid_file):
            os.remove(pid_file)
