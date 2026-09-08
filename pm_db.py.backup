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

def get_db_connection(retry=0):
    try:
        pool = init_db_pool()
        return pool.get_connection()
    except mysql.connector.Error as e:
        if retry < 2:
            time.sleep(0.1 * (2 ** retry))
            return get_db_connection(retry + 1)
        raise

def db_function(case, PMID=0, x=0, y=0, z=0, a=0, b=0):
    for attempt in range(MAX_RETRIES):
        mydb = None
        cursor = None
        try:
            mydb = get_db_connection()
            cursor = mydb.cursor()

            # MQTT Subscribe
            if case == "db_mqttsub":
                cursor.execute("SELECT meterID FROM PM_user WHERE meterType < 5")
                result = cursor.fetchall()
                return result

            # 檢查ID
            elif case == "db_idcheck":
                sql = "SELECT 1 FROM PM_user Where meterID Like %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result

            # esp8266 say hello
            elif case == "db_hello":
                sql = "UPDATE PM_user SET conState = '1' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"

            # esp8266 online check
            elif case == "db_online_prepay":
                meterType = ["1", "2", "3", "4", "9"]
                placeholders = ','.join(['%s'] * len(meterType))
                sql = f"SELECT meterID FROM PM_user WHERE conState = '1' AND onlineDate IS NOT NULL \
                    AND meterType in ({placeholders})"
                cursor.execute(sql, meterType)
                result = cursor.fetchall()
                return result

            # 8266 no feedback
            elif case == "db_mqtterr":
                sql = "UPDATE PM_user SET conState = '0' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"

            # 取得電量結算暫存
            elif case == "db_tempkwh":
                sql = "UPDATE PM_user SET tempKWh = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"

            # 取得電量結算暫
            elif case == "db_tempkwhget":
                sql = "SELECT tempKWh FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]

            # 取得餘額結算暫存
            elif case == "db_tempbal":
                sql = "UPDATE PM_user SET tempBal = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                sql = "SELECT meterName FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]  # meterName

            # 取得餘額結算暫存並清空
            elif case == "db_tempbalget":
                sql = "SELECT tempBal FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                sql = "UPDATE PM_user SET tempBal = NULL WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return result[0]

            # 修改供電狀態
            elif case == "db_stateset":
                sql = "UPDATE PM_user SET meterState = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"

            # 取得供電狀態
            elif case == "db_stateget":
                sql = "SELECT meterState FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]
            
            # 更新供電狀態 23.07.14 -> 先確認是否為假消息，查詢是否有供斷電指令
            # 24.12.27 breaker不需要確認狀態
            elif case == "db_state_update": # meterID, state
                sql = "SELECT id, meterState, meterBreaker FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if result[2] == 1: # 24.12.27 breaker不需要確認狀態
                    cursor.execute("UPDATE PM_user SET meterState = %s, switchWait = 0 WHERE meterID = %s", (x, PMID))
                    mydb.commit()
                elif result[1] == -1:
                    sql = "SELECT RIGHT(action, 2) FROM PM_user_actionlist WHERE user_id = %s AND remark = 'meterstate' \
                        ORDER BY id DESC LIMIT 1"
                    cursor.execute(sql, (result[0],))
                    result = cursor.fetchone()
                    if not result or result[0] == "供電": x = "1"
                    elif result[0] == "斷電": x = "0"
                    cursor.execute("UPDATE PM_user SET meterState = %s WHERE meterID = %s", (x, PMID))
                    mydb.commit()
                elif str(result[1]) != x:
                    sql = "SELECT RIGHT(action, 2) FROM PM_user_actionlist WHERE user_id = %s AND remark = 'meterstate' \
                        ORDER BY id DESC LIMIT 1"
                    cursor.execute(sql, (result[0],))
                    result = cursor.fetchone()
                    if result: 
                        if (result[0] == "供電" and x == "1") or (result[0] == "斷電" and x == "0"):
                            cursor.execute("UPDATE PM_user SET meterState = %s WHERE meterID = %s", (x, PMID))
                            mydb.commit()

            # 供電狀態switch waiting, 1 = 等待中 / -1 = 切換失敗 / 0 = done or nothing
            # 23.07.12 儲存供斷電執行紀錄
            elif case == "db_switchset":
                sql = "UPDATE PM_user SET switchWait = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                now_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
                sql = "SELECT id, client_id FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if x == "1": # y = wait / z = action / a = KEY
                    sql = "INSERT INTO PM_user_actionlist (user_id, client_id, time, action, remark, api_key) \
                        VALUES (%s, %s,%s, %s, %s, %s)"
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

            # 讀取供電切換switchWait狀態
            elif case == "db_switchget":
                sql = "SELECT switchWait FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                return result[0]

            # 修改扣費模式
            elif case == "db_typeset":
                sql = "UPDATE PM_user SET countType = %s WHERE meterID = %s"
                cursor.execute(sql, (x, PMID))
                mydb.commit()
                return "ok"

            # 原電量測試找Bug -> API 使用紀錄
            # 改寫單位電量變更API -> 加入actionnote作為變更紀錄
            # 25.08.07 暫時做法：判斷與舊有數據資料長度是否差10倍
            elif case == "db_kwhtest" or case == "db_actionnote": # x=nt, y=raw data , z= value 數值
                sql = "SELECT id, tempKWh FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                user_id, kwh = result
                if len(z) > len(kwh) and (int(z) // int(kwh)) >= 10: z = z[:-1] # 25.08.07
                sql = "INSERT INTO PM_test (user_id,time,data,value) VALUES (%s,%s,%s,%s)"
                cursor.execute(sql, (str(user_id), x, y, z))
                mydb.commit()
                return "ok"

            # 改寫單位電量變更API -> 加入acionnote作為變更紀錄（變更完成）
            elif case == "db_actionget": # data = actionNo or delay 1min
                sql = "SELECT data, value FROM PM_test WHERE (data = %s or data = %s) \
                    AND value <> 'Success' limit 1"
                cursor.execute(sql, (x, y))
                result = cursor.fetchone()
                sql = "UPDATE PM_test SET value = 'Success' WHERE data = %s"
                cursor.execute(sql, (result[0],))
                mydb.commit()
                return result[1] if result else None

            # 改寫單位電量變更API -> 加入acionnote作為變更紀錄（變更失敗）
            elif case == "db_actionerror":
                sql = "UPDATE PM_test SET value = 'err0' WHERE data = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"

            # 改寫單位電量變更API -> 查詢是否變更完成
            elif case == "db_actioncheck":
                sql = "SELECT value FROM PM_test WHERE data = %s"
                cursor.execute(sql, (x,))
                result = cursor.fetchone()
                return result[0]

            # 改寫單位電費變更API -> 設定單位電價
            elif case == "db_unitset":
                sql = "UPDATE PM_user SET unitPrice = %s WHERE meterID = %s"
                cursor.execute(sql, (int(x)/100, PMID))
                mydb.commit()
                return "ok"

            # 清除餘額
            elif case == "db_zeromoney":
                sql = "UPDATE PM_user SET tempBal = '0' WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                mydb.commit()
                return "ok"
            
            # API執行『預約動作』前，查詢KEY
            elif case == "db_keycheck": # KEY
                sql = "SELECT APIUser FROM APIlist WHERE APIKey = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if not result: return False
                return True
            
            # 取得設備連線狀態（僅支援客戶API串接，門禁後台未使用）22.11.16
            elif case == "db_get_conn_state": # PMID
                sql = "SELECT conState FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if not result: return "err2"
                elif result[0] == 0: return "0"
                return "1"
                
            # ---------------- 電表Feedback / pending check ----------------
            # 24.10.16 加入PM_pending_list, 由power server 重新執行確認
            elif case == "add_meter_pending_job": # QRID, act = on(N) / off(O)
                sql = "SELECT id, meterType FROM PM_user WHERE meterID = %s"
                cursor.execute(sql, (PMID,))
                result = cursor.fetchone()
                if result and result[1] != 1: # 1 = 一般計量型電表，無法切換供電狀態
                    user_id = result[0]
                    act = 1 if x == "N" else 0
                    opposite_act = 0 if act == 1 else 1
                    sql = "UPDATE PM_pending_list SET closeAt = DATE_ADD(now(), INTERVAL 8 HOUR) \
                            WHERE user_id = %s AND action = %s AND closeAt IS NULL"
                    cursor.execute(sql, (user_id, opposite_act))
                    sql = """INSERT INTO PM_pending_list (user_id, createAt, action)
                            SELECT %s, DATE_ADD(NOW(), INTERVAL 8 HOUR), %s FROM DUAL
                            WHERE NOT EXISTS (
                                SELECT 1 FROM PM_pending_list WHERE user_id = %s
                                AND action = %s AND closeAt IS NULL)"""
                    cursor.execute(sql, (user_id, act, user_id, act))
                    mydb.commit()
                
            # 24.10.16 檢查meter_pending, feedback更新狀態 / 有新的控制指令待檢查
            elif case == "update_meter_pending_job": # PMID, act = on(1) / off(0)
                sql = "SELECT b.id, b.tempKWh FROM PM_pending_list a INNER JOIN PM_user b ON a.user_id = b.id \
                        WHERE b.meterID = %s AND a.action = %s AND a.closeAt IS NULL LIMIT 1"
                cursor.execute(sql, (PMID, x)) 
                result = cursor.fetchone()
                if result:
                    user_id, tempKWh = result
                    sql = "UPDATE PM_pending_list SET closeAt = DATE_ADD(now(), INTERVAL 8 HOUR) \
                            WHERE user_id = %s AND closeAt IS NULL"
                    cursor.execute(sql, (user_id,))
                    _time = datetime.now(tz).strftime('%H:%M:%S')
                    _act = " 恢復供電" if x == "1" else " 停止供電"
                    remark = f"{_time} {_act}"
                    sql = "INSERT INTO PM_costlist (user_id, listNo, listDate, cumKWh, remark) \
                        VALUES (%s, 'meterstate', DATE_ADD(now(), INTERVAL 8 HOUR), %s, %s)"
                    cursor.execute(sql, (user_id, tempKWh, remark))
                    mydb.commit()
                return "ok"
            

            except Error as e:
                if mydb:
                    mydb.rollback()

                error_msg = str(e)
                is_lock_timeout = "1205" in error_msg or "Lock wait timeout" in error_msg

                if is_lock_timeout and attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"[{case}] 鎖定超時，{wait_time:.2f} 秒後重試 ({attempt + 1}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[{case}] 資料庫操作失敗: {e}")
                    return None

            finally:
                if cursor:
                    cursor.close()
                if mydb:
                    mydb.close()

            return "ok"

        except Error as e:
            logger.error(f"[{case}] 最終失敗: {e}")
            return None

