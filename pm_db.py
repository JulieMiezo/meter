import mysql.connector
from mysql.connector import Error, pooling
from datetime import datetime, timezone, timedelta
import pytz, json, os, logging, time
tz = pytz.timezone('Asia/Taipei')
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_pool = None
MAX_RETRIES = 3
RETRY_DELAY = 0.5

def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pooling.MySQLConnectionPool(
            pool_name="mqttpool",
            pool_size=5,
            pool_reset_session=True,
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            autocommit=False,
            connection_timeout=15,
            get_warnings=False
        )
    return db_pool

def _execute_query(query_func, case_name):
    """
    執行查詢並自動重試鎖定超時錯誤
    query_func: 執行查詢的函數，接收 mydb 和 cursor
    """
    for attempt in range(MAX_RETRIES):
        mydb = None
        cursor = None
        try:
            pool = init_db_pool()
            mydb = pool.get_connection()
            cursor = mydb.cursor()

            result = query_func(mydb, cursor)
            return result

        except Error as e:
            if mydb:
                try:
                    mydb.rollback()
                except:
                    pass

            error_msg = str(e)
            is_lock_timeout = "1205" in error_msg or "Lock wait timeout" in error_msg

            if is_lock_timeout and attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"[{case_name}] 鎖定超時，{wait_time:.2f} 秒後重試 ({attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                logger.error(f"[{case_name}] 資料庫操作失敗: {e}")
                return None

        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if mydb:
                try:
                    mydb.close()
                except:
                    pass

def db_function(case, PMID=0, x=0, y=0, z=0, a=0, b=0):
    try:
        # MQTT Subscribe
        if case == "db_mqttsub":
            def query(mydb, cursor):
                cursor.execute("SELECT meterID FROM PM_user WHERE meterType < 5")
                return cursor.fetchall()
            return _execute_query(query, case)

        # 檢查ID
        elif case == "db_idcheck":
            def query(mydb, cursor):
                sql = "SELECT 1 FROM PM_user Where meterID Like %s"
                cursor.execute(sql, (PMID,))
                return cursor.fetchone()
            return _execute_query(query, case)

        # esp8266 say hello
        elif case == "db_hello":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET conState = '1' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # esp8266 online check
        elif case == "db_online_prepay":
            def query(mydb, cursor):
                meterType = ["1", "2", "3", "4", "9"]
                placeholders = ','.join(['%s'] * len(meterType))
                sql = f"SELECT meterID FROM PM_user WHERE conState = '1' AND onlineDate IS NOT NULL AND meterType in ({placeholders})"
                cursor.execute(sql, meterType)
                return cursor.fetchall()
            return _execute_query(query, case)

        # 8266 no feedback
        elif case == "db_mqtterr":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET conState = '0' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 取得電量結算暫存
        elif case == "db_tempkwh":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET tempKWh = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 取得電量結算暫
        elif case == "db_tempkwhget":
            def query(mydb, cursor):
                sql = "SELECT tempKWh FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]
            return _execute_query(query, case)

        # 取得餘額結算暫存
        elif case == "db_tempbal":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET tempBal = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                sql = "SELECT meterName FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]
            return _execute_query(query, case)

        # 取得餘額結算暫存並清空
        elif case == "db_tempbalget":
            def query(mydb, cursor):
                sql = "SELECT tempBal FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                sql = "UPDATE PM_user SET tempBal = NULL WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return result[0]
            return _execute_query(query, case)

        # 修改供電狀態
        elif case == "db_stateset":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET meterState = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 取得供電狀態
        elif case == "db_stateget":
            def query(mydb, cursor):
                sql = "SELECT meterState FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]
            return _execute_query(query, case)

        # 更新供電狀態
        elif case == "db_state_update":
            def query(mydb, cursor):
                sql = "SELECT id, meterState, meterBreaker FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if result[2] == 1:
                    cursor.execute("UPDATE PM_user SET meterState = %s, switchWait = 0 WHERE meterID = %s", (x, PMID))
                    mydb.commit()
                elif result[1] == -1:
                    sql = "SELECT RIGHT(action, 2) FROM PM_user_actionlist WHERE user_id = %s AND remark = 'meterstate' ORDER BY id DESC LIMIT 1"
                    cursor.execute(sql, (result[0],))
                    action_result = cursor.fetchone()
                    if not action_result or action_result[0] == "供電": x_val = "1"
                    elif action_result[0] == "斷電": x_val = "0"
                    else: x_val = x
                    cursor.execute("UPDATE PM_user SET meterState = %s WHERE meterID = %s", (x_val, PMID))
                    mydb.commit()
                elif str(result[1]) != x:
                    sql = "SELECT RIGHT(action, 2) FROM PM_user_actionlist WHERE user_id = %s AND remark = 'meterstate' ORDER BY id DESC LIMIT 1"
                    cursor.execute(sql, (result[0],))
                    action_result = cursor.fetchone()
                    if action_result:
                        if (action_result[0] == "供電" and x == "1") or (action_result[0] == "斷電" and x == "0"):
                            cursor.execute("UPDATE PM_user SET meterState = %s WHERE meterID = %s", (x, PMID))
                            mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 供電狀態switch waiting
        elif case == "db_switchset":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET switchWait = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                now_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
                sql = "SELECT id, client_id FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if x == "1":
                    sql = "INSERT INTO PM_user_actionlist (user_id, client_id, time, action, remark, api_key) VALUES (%s, %s,%s, %s, %s, %s)"
                    cursor.execute(sql, (result[0], result[1], now_time, z, y, a))
                    mydb.commit()
                elif x == "-1":
                    sql = "UPDATE PM_user_actionlist SET remark = 'no feedback' WHERE user_id = %s AND remark = 'wait'"
                    cursor.execute(sql, (result[0],))
                    mydb.commit()
                elif x == "0" and y == "meterstate":
                    sql = "UPDATE PM_user_actionlist SET remark = 'meterstate' WHERE user_id = %s AND remark = 'wait'"
                    cursor.execute(sql, (result[0],))
                    mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 讀取供電切換switchWait狀態
        elif case == "db_switchget":
            def query(mydb, cursor):
                sql = "SELECT switchWait FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]
            return _execute_query(query, case)

        # 修改扣費模式
        elif case == "db_typeset":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET countType = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 原電量測試找Bug / API 使用紀錄
        elif case == "db_kwhtest" or case == "db_actionnote":
            def query(mydb, cursor):
                sql = "SELECT id, tempKWh FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                user_id, kwh = result
                z_val = z
                if len(z_val) > len(kwh) and (int(z_val) // int(kwh)) >= 10:
                    z_val = z_val[:-1]
                sql = "INSERT INTO PM_test (user_id,time,data,value) VALUES (%s,%s,%s,%s)"
                cursor.execute(sql, (str(user_id), x, y, z_val))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 改寫單位電量變更API
        elif case == "db_actionget":
            def query(mydb, cursor):
                sql = "SELECT data, value FROM PM_test WHERE (data = %s or data = %s) AND value <> 'Success' limit 1"
                cursor.execute(sql, (x, y))
                result = cursor.fetchone()
                if result:
                    sql = "UPDATE PM_test SET value = 'Success' WHERE data = %s"
                    cursor.execute(sql, (result[0],))
                    mydb.commit()
                    return result[1]
                return None
            return _execute_query(query, case)

        # 改寫單位電量變更API - 失敗
        elif case == "db_actionerror":
            def query(mydb, cursor):
                sql = "UPDATE PM_test SET value = 'err0' WHERE data = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 查詢是否變更完成
        elif case == "db_actioncheck":
            def query(mydb, cursor):
                sql = "SELECT value FROM PM_test WHERE data = %s"
                cursor.execute(sql, (x,))
                result = cursor.fetchone()
                return result[0]
            return _execute_query(query, case)

        # 設定單位電價
        elif case == "db_unitset":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET unitPrice = %s WHERE meterID = %s"
                cursor.execute(sql, (int(x)/100, PMID))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # 清除餘額
        elif case == "db_zeromoney":
            def query(mydb, cursor):
                sql = "UPDATE PM_user SET tempBal = '0' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # API執行『預約動作』前，查詢KEY
        elif case == "db_keycheck":
            def query(mydb, cursor):
                sql = "SELECT APIUser FROM APIlist WHERE APIKey = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result is not None
            return _execute_query(query, case)

        # 取得設備連線狀態
        elif case == "db_get_conn_state":
            def query(mydb, cursor):
                sql = "SELECT conState FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if not result:
                    return "err2"
                elif result[0] == 0:
                    return "0"
                return "1"
            return _execute_query(query, case)

        # Pending list 操作
        elif case == "add_meter_pending_job":
            def query(mydb, cursor):
                sql = "SELECT id, meterType FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if result and result[1] != 1:
                    user_id = result[0]
                    act = 1 if x == "N" else 0
                    opposite_act = 0 if act == 1 else 1
                    sql = "UPDATE PM_pending_list SET closeAt = DATE_ADD(now(), INTERVAL 8 HOUR) WHERE user_id = %s AND action = %s AND closeAt IS NULL"
                    cursor.execute(sql, (user_id, opposite_act))
                    sql = """INSERT INTO PM_pending_list (user_id, createAt, action)
                            SELECT %s, DATE_ADD(NOW(), INTERVAL 8 HOUR), %s FROM DUAL
                            WHERE NOT EXISTS (SELECT 1 FROM PM_pending_list WHERE user_id = %s AND action = %s AND closeAt IS NULL)"""
                    cursor.execute(sql, (user_id, act, user_id, act))
                    mydb.commit()
                return "ok"
            return _execute_query(query, case)

        # Pending list 檢查更新
        elif case == "update_meter_pending_job":
            def query(mydb, cursor):
                sql = "SELECT b.id, b.tempKWh FROM PM_pending_list a INNER JOIN PM_user b ON a.user_id = b.id WHERE b.meterID = %s AND a.action = %s AND a.closeAt IS NULL LIMIT 1"
                cursor.execute(sql, (PMID, x))
                result = cursor.fetchone()
                if result:
                    user_id, tempKWh = result
                    sql = "UPDATE PM_pending_list SET closeAt = DATE_ADD(now(), INTERVAL 8 HOUR) WHERE user_id = %s AND closeAt IS NULL"
                    cursor.execute(sql, (user_id,))
                    _time = datetime.now(tz).strftime('%H:%M:%S')
                    _act = " 恢復供電" if x == "1" else " 停止供電"
                    remark = f"{_time} {_act}"
                    sql = "INSERT INTO PM_costlist (user_id, listNo, listDate, cumKWh, remark) VALUES (%s, 'meterstate', DATE_ADD(now(), INTERVAL 8 HOUR), %s, %s)"
                    cursor.execute(sql, (user_id, tempKWh, remark))
                    mydb.commit()
                return "ok"
            return _execute_query(query, case)

    except Exception as e:
        logger.error(f"[{case}] 未預期的錯誤: {e}")
        return None
