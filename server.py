# CI/CD Test - 2026-07-13 - Sudoers configured - full CI/CD working!
# ----- import Miézo library -----
import pm_db as pmdb
import pm_fn as pmfn
import config as con
# ----- import System library -----
import cherrypy, os ,requests
import pytz, time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta, date
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
client = mqtt.Client()
tz = pytz.timezone('Asia/Taipei')
scheduler = BackgroundScheduler(job_defaults={'coalesce': True, 'max_instances': 1})
scheduler.start()


DOORID = 0
KWH_DATA = 'err0'
VOL_DATA = 'err0'
AMP_DATA = 'err0'
FRE_DATA = 'err0'
BAL_DATA = 'err0'  # Balance / Add money
UNI_DATA = 'err0'  # Unit Price
RST_DATA = 'err0'  # Relay State

mqttserver = os.getenv("MQTTSERVER") 
serverno = os.getenv("SERVER_NO")
check_relay = int(os.getenv("CHECK_RELAY"))


timeshort = 1
timelong = 3


# ----------------- / 改版紀錄 / -----------------
# 2023.06.27 刪除 kwh_job -> 已移至pm-mqtt server 執行
# 2024.11.21 電表 Breaker 仿冷氣做法：沒有收到Feedback時加入 PM_pending_list，收到feedback後執行update_pending_job
# 2025.01.03 活力冷氣流程
# 2025.02.18 移除活力冷氣流程，回歸門禁server獨立完成

def mqtt_err_job(x, y, z=""): # x = meterID, y = case, z = actionNo
    remove_job('mqtt_err_job_'+x+y)
    nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    if y == 'e': # get KWh
        pmdb.db_function("db_tempkwh", x, 'err0')
    elif y == 'N' or y == 'O': # set on / off
        pmdb.db_function("db_switchset", x, '-1')
        pmdb.db_function("db_stateset", x, '-1')
        scheduler.add_job(clear_job, 'interval', seconds=1, id='clear_job_'+x, args=[x, y], replace_existing=True)
    elif y == 'u': # set unit price
        pmdb.db_function("db_actionerror", z)
    elif y == 'r': # get Relay State
        pmdb.db_function("db_stateset", x, '-1')
    else: pmdb.db_function("db_kwhtest", x, nt, y, "err0")
    print(x, '連線失敗', nt, y)
    # qrfn.sendemail(x,"mqtterr")


# 24.11.21 Breaker 沒有 Feedback -> PM_pending_list
def clear_job(x, y): # x = meterID, y = N / O /err
    remove_job('clear_job_'+x)
    nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    pmdb.db_function("db_switchset", x, '0')
    if y != "err": pmdb.db_function("add_meter_pending_job", x, y)
    print(x, 'Clear', nt)

# 23.03.27 改為每6小時更新一次
# 25.06.13 分批次執行，每 2 小時執行一次
def relay_job(): 
    case = "r"
    result = pmdb.db_function("db_online_prepay")
    
    global check_relay
    _start = check_relay
    _end = check_relay + 100
    end_round = False
    if _end > len(result): 
        end_round = True
        _end = len(result)
    print(_start, _end, end_round, len(result))
    
    for x in result[_start: _end]:
        PMID = x[0]
        scheduler.add_job(mqtt_err_job, 'interval', seconds=15,
                            id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True, )
        publish.single(PMID, "relay"+serverno, hostname=mqttserver, port=8083)
        
    check_relay = 0 if end_round else _end


def bal_job(): # 月結方案自動加值未完成
    print("bal_job")
    case = 'b'
    result = pmdb.db_function("db_online_prepay_autoadd")
    for x in range(len(result)):
        PMID = result[x][2]
        scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort,
                            id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
        publish.single(PMID, "baldata"+serverno, hostname=mqttserver, port=8083)

def remove_job(x): # x = job id
    for job in scheduler.get_jobs():
        if job.id.startswith(x):scheduler.remove_job(job.id)

def switchstart(x): # 按鈕恢復供電
    Key = "QI3M8Q"
    data = {"QRID": x, "KEY": Key}
    requests.get('https://miezo-qrlock.com.tw/meter/switchstart?', data)

def switchend(x): # 按鈕停止供電
    Key = "QI3M8Q"
    data = {"QRID": x, "KEY": Key}
    requests.get('https://miezo-qrlock.com.tw/meter/switchend?', data)

# # ----------------------------- [ 其他應用 ] -----------------------------


# ----------------------------- [ MQTT ] -----------------------------
def on_log(client, userdata, level, buf):
        print("log: ",buf)

def do_subscribe():
    client.subscribe("AddNewMeter")
    client.subscribe("pm-0"+serverno[-1])
    result = pmdb.db_function("db_mqttsub")
    for x in result:
        tempID = x[0]
        client.subscribe(tempID+"/feedback"+serverno)
    print("MQTT subscribed:", len(result), "meters")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT on connect successfully.")
        do_subscribe()
        scheduler.add_job(relay_job, 'cron', minute=11, id='relay_job', replace_existing=True)
        scheduler.add_job(bal_job, 'interval', weeks=4, id='bal_job', replace_existing=True)
    else:
        print(f"MQTT connect failed with code: {rc}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"MQTT disconnected unexpectedly, code: {rc}")
    else:
        print("MQTT disconnected cleanly")

def on_message(client, userdata, msg):
    print(msg.topic, msg.payload)
    getID = msg.topic[0:18]
    getCase = msg.payload[0:3]
    nt = datetime.now(tz)
    if msg.topic == "AddNewMeter":
        getID = msg.payload.decode()
        client.subscribe(getID+"/feedback"+serverno)
    if getCase == b'pmE':
        global KWH_DATA
        KWH_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'e')
        if getID[0:2] == "45" and "&" in KWH_DATA:
            data = KWH_DATA.split("&")[1]
            value = KWH_DATA.split("&")[0]
            nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
            if data[0:3] != "132":
                pmdb.db_function("db_tempkwh", getID, value)
            pmdb.db_function("db_kwhtest", getID, nt, data, value)
        else: pmdb.db_function("db_tempkwh", getID, KWH_DATA)
        print(getID, '即時電量', nt, KWH_DATA)

    elif getCase == b'pmV':
        global VOL_DATA
        VOL_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'v')
        print(getID, '即時電壓', nt, VOL_DATA)

    elif getCase == b'pmA':
        global AMP_DATA
        AMP_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'a')
        print(getID, '即時電流', nt, AMP_DATA)

    elif getCase == b'pmF':
        global FRE_DATA
        FRE_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'f')
        print(getID, '即時頻率', nt, FRE_DATA)

    elif getCase == b'pmB':
        global BAL_DATA
        BAL_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'b')
        pmdb.db_function("db_tempbal", getID, BAL_DATA)
        print(getID, '剩餘金額', nt, BAL_DATA)

    elif getCase == b'pmM':
        BAL_DATA = msg.payload[3:].decode()
        remove_job('mqtt_err_job_'+getID+'m')
        meterName = pmdb.db_function("db_tempbal", getID, BAL_DATA)
        if int(BAL_DATA)/100 < 1000:
            pmfn.sendemail(getID, "balalert", meterName, BAL_DATA)
        print(getID, '加值成功，剩餘金額', nt, BAL_DATA)

    elif getCase == b'pmU':
        now = datetime.now(tz)
        actionNo = "U"+now.strftime('%Y%m%d%H%M')
        actionNo_2 = "U"+(now+timedelta(minutes=-1)).strftime('%Y%m%d%H%M')
        remove_job('mqtt_err_job_'+getID+'u')
        unit = pmdb.db_function("db_actionget", getID, actionNo, actionNo_2)
        if unit.isdigit() == True: pmdb.db_function("db_unitset", getID, unit)
        print(getID, '變更成功，單位電價', nt)

    elif getCase == b'pmD':
        remove_job('mqtt_err_job_'+getID+'D')
        s = msg.payload[3:].decode().split('&')
        print(s)
        KWH_DATA = s[0]
        VOL_DATA = s[1]
        AMP_DATA = s[2]
        FRE_DATA = s[3]
        print(getID, '即時數據', nt, KWH_DATA, VOL_DATA, AMP_DATA, FRE_DATA)

    elif getCase == b'pmR':
        if msg.payload[3] == 49:
            result = "ON"
            pmdb.db_function("db_state_update", getID, '1') # 23.07.14
            pmdb.db_function("update_meter_pending_job", getID, 1)

        elif msg.payload[3] == 50:
            result = "OFF"
            pmdb.db_function("db_state_update", getID, '0') # 23.07.14
            pmdb.db_function("update_meter_pending_job", getID, 0)

        else: result = "err0"
        remove_job('mqtt_err_job_'+getID+'r')
        print(getID, 'Relay狀態', nt, result)

    elif getCase == b'pmN':
        remove_job('mqtt_err_job_'+getID+'N')
        if msg.payload[3] == 49:
            result = "ON"
            pmdb.db_function("db_switchset", getID, '0', "meterstate") # 23.07.12 儲存供斷電執行紀錄
            pmdb.db_function("db_stateset", getID, '1')
            pmdb.db_function("update_meter_pending_job", getID, 1)

        else:
            result = "err0"
            scheduler.add_job(clear_job, 'interval', seconds=1, id='clear_job_'+getID, args=[getID, "err"], replace_existing=True)
        print(getID, '恢復供電', nt, result)

    elif getCase == b'pmO':
        remove_job('mqtt_err_job_'+getID+'O')
        if msg.payload[3] == 50:
            result = "OFF"
            pmdb.db_function("db_switchset", getID, '0', "meterstate") # 23.07.12 儲存供斷電執行紀錄
            pmdb.db_function("db_stateset", getID, '0')
            pmdb.db_function("update_meter_pending_job", getID, 0)

        else:
            result = "err0"
            scheduler.add_job(clear_job, 'interval', seconds=1, id='clear_job_'+getID, args=[getID, "err"], replace_existing=True)
        print(getID, '停止供電', nt, result)

    elif getCase == b'pmJ':
        time = (nt+timedelta(seconds=+1)).strftime('%Y-%m-%d %H:%M:%S')
        scheduler.add_job(switchstart, 'date', run_date=time, args=[getID], timezone=tz, replace_existing=True)
        print(getID, '按鈕恢復供電', nt, "ON")

    elif getCase == b'pmQ':
        time = (nt+timedelta(seconds=+1)).strftime('%Y-%m-%d %H:%M:%S')
        scheduler.add_job(switchend, 'date', run_date=time, args=[getID], timezone=tz, replace_existing=True)
        print(getID, '按鈕關閉供電', nt, "OFF")

    elif getCase == b'pmZ':
        remove_job('mqtt_err_job_'+getID+'z')
        pmdb.db_function("db_zeromoney", getID)
        print(getID, '成功清除餘額', nt)

    elif getCase == b'pmT':
        remove_job('mqtt_err_job_'+getID+'t')
        pmdb.db_function("db_typeset", getID, 0)
        print(getID, '電表扣費設定完成', nt)

    elif getCase == b'pmP':
        remove_job('mqtt_err_job_'+getID+'p')
        pmdb.db_function("db_typeset", getID, 1)
        print(getID, '讀卡機扣費設定完成', nt)

    elif msg.payload == b'errE':
        remove_job('mqtt_err_job_'+getID+'e')
        pmdb.db_function("db_tempkwh", getID, 'err1')
        print(getID, '電量讀取錯誤')

    elif msg.payload == b'errV':
        VOL_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'v')
        print(getID, '電壓讀取錯誤')

    elif msg.payload == b'errA':
        AMP_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'a')
        print(getID, '電流讀取錯誤')

    elif msg.payload == b'errF':
        FRE_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'f')
        print(getID, '頻率讀取錯誤')

    elif msg.payload == b'errB':
        BAL_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'b')
        print(getID, '餘額讀取錯誤')

    elif msg.payload == b'errM':
        BAL_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'m')
        print(getID, '加值連線錯誤')

    elif msg.payload == b'errU':
        UNI_DATA = 'err1'
        remove_job('mqtt_err_job_'+getID+'u')
        print(getID, '變更連線錯誤')

    elif msg.payload == b'clear':
        KWH_DATA = '0'
        remove_job('mqtt_err_job_'+getID+'c')
        print(getID, '電量歸零', nt)

    elif msg.payload == b'errR':
        remove_job('mqtt_err_job_'+getID+'r')
        print(getID, '狀態讀取錯誤')

    elif msg.payload == b'errZ':
        remove_job('mqtt_err_job_'+getID+'z')
        print(getID, '清除餘額錯誤')
        
    # --------------- 系統 Feedback ---------------
    elif msg.payload == b'MQTTcheck': #from monitor server
        server = msg.topic
        publish.single(server+"/feedback", server, hostname=mqttserver, port=8083)



class Root(object):
    @cherrypy.expose
    def do_contact(self, **params):
        return 'Hello World!'
    
    # 前端發送Request歸零電量 / 一般計量型電表
    @cherrypy.expose
    def cleardata_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        print("cleardata_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'c'
            global KWH_DATA
            KWH_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=3, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "zerodata"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request清除餘額 / 儲值型電表
    @cherrypy.expose
    def zeromoney_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        print("zeromoney_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'z'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=3, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "zerodata"+serverno, hostname=mqttserver, port=8083)

    # ---------------------------------
    # 前端發送Request請求最新用電量
    @cherrypy.expose
    def kwh_request(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        elif PMID[0:2] == "45" and KEY != "QI3M8Q":  # from QRLock CMS
            return nt+"&err1"
        else:
            case = 'e'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "kwhdata"+serverno, hostname=mqttserver, port=8083)


    # 前端發送Request取得最新用電量
    @cherrypy.expose
    def kwh_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else:
            KWH_DATA = pmdb.db_function("db_tempkwhget", PMID)
            if KWH_DATA == None: KWH_DATA = "err1"
            return nt+"&"+KWH_DATA

    # ---------------------------------
    # 前端發送Request請求電壓
    @cherrypy.expose
    def vol_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        # print("vol_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'v'
            global VOL_DATA
            VOL_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "voldata"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得電壓
    @cherrypy.expose
    def vol_get(self, PMID, KEY=""):
        # print("KWH_data_get")
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else: return nt+"&"+VOL_DATA

    # ---------------------------------
    # 前端發送Request請求電流
    @cherrypy.expose
    def amp_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        # print("amp_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'a'
            global AMP_DATA
            AMP_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "ampdata"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得電流
    @cherrypy.expose
    def amp_get(self, PMID, KEY=""):
        # print("KWH_data_get")
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else: return nt+"&"+AMP_DATA

    # ---------------------------------
    # 前端發送Request請求頻率
    @cherrypy.expose
    def fre_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        # print("fre_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'f'
            global FRE_DATA
            FRE_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "fredata"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得頻率
    @cherrypy.expose
    def fre_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else: return nt+"&"+FRE_DATA

# ---------------------------------
    # 前端發送Request請求餘額
    @cherrypy.expose
    def bal_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'b'
            global BAL_DATA
            BAL_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "baldata"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得餘額
    @cherrypy.expose
    def bal_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else:
            BAL_DATA = pmdb.db_function("db_tempbalget", PMID)
            if BAL_DATA == None:
                BAL_DATA = "err1"
            return nt+"&"+BAL_DATA

# ---------------------------------
    # 前端發送Request請求加值(0~99999)
    @cherrypy.expose
    def money_request(self, PMID, VALUE, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        # result = PM_user all, meterState = result[6]
        if not result: return "err2"
        else:
            l = len(VALUE)
            if l == 1:
                VALUE = "0000"+VALUE
            elif l == 2:
                VALUE = "000"+VALUE
            elif l == 3:
                VALUE = "00"+VALUE
            elif l == 4:
                VALUE = "0"+VALUE
            case = 'm'
            global BAL_DATA
            BAL_DATA = 'err0'
            METER = str(result[6])
            if METER == "0":
                cmd = "m0ney"+VALUE+serverno
            else:
                cmd = "m1ney"+VALUE+serverno
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timelong, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, cmd, hostname=mqttserver, port=8083)

    # ---------------------------------
    # 前端發送Request請求所有數據
    @cherrypy.expose
    def data_request(self, PMID, KEY=""):
        nt = datetime.now(tz)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            case = 'D'
            global KWH_DATA
            KWH_DATA = 'err0'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=1.5, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "data"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得所有數據
    @cherrypy.expose
    def data_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        elif KWH_DATA[0:3] == 'err': return nt+"&"+KWH_DATA
        return nt+"&"+KWH_DATA+"&"+VOL_DATA+"&"+AMP_DATA+"&"+FRE_DATA

    # ---------------------------------
    # 前端發送Request請求供電狀態
    @cherrypy.expose
    def pwstate_request(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        # print("pwstate_request", nt)
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        elif PMID[0:2] == "45" and KEY != "QI3M8Q":  # from QRLock
            return nt+"&err1"
        elif PMID[0:2] == "45" or PMID[0:2] == "42":
            case = 'r'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timeshort, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            publish.single(PMID, "relay"+serverno, hostname=mqttserver, port=8083)

    # 前端發送Request取得供電狀態
    @cherrypy.expose
    def pwstate_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        elif PMID[0:2] == "65": return nt+"&err1"
        # elif PMID[0:2] == "45":
        else:
            RST_DATA = pmdb.db_function("db_stateget", PMID)
            if RST_DATA == 0: RST_DATA = "OFF"
            elif RST_DATA == 1: RST_DATA = "ON"
            elif RST_DATA == -1: RST_DATA = "err"
            return nt+"&"+RST_DATA

    # ---------------------------------
    # 前端發送Request請求供電
    @cherrypy.expose
    def pwon_request(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            switchWait = pmdb.db_function("db_switchget", PMID)
            if switchWait != 1:
                for x in range(len(scheduler.get_jobs())):
                    if PMID in scheduler.get_jobs()[x].id:
                        return nt+"&busy"
                case = 'N'
                scheduler.add_job(mqtt_err_job, 'interval', seconds=timelong, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
                pmdb.db_function("db_switchset", PMID, '1', "wait", "電表供電", KEY) # 23.07.12 儲存供斷電執行紀錄
                publish.single(PMID, "Npwon"+serverno, hostname=mqttserver, port=8083)


    # ---------------------------------
    # 前端發送Request請求斷電
    @cherrypy.expose
    def pwoff_request(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        switchWait = pmdb.db_function("db_switchget", PMID)
        if switchWait != 1:
            for x in range(len(scheduler.get_jobs())):
                if PMID in scheduler.get_jobs()[x].id:
                    return nt+"&busy"
            case = 'O'
            scheduler.add_job(mqtt_err_job, 'interval', seconds=timelong, id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
            pmdb.db_function("db_switchset", PMID, '1', "wait", "電表斷電", KEY) # 23.07.12 儲存供斷電執行紀錄
            publish.single(PMID, "Opwoff"+serverno, hostname=mqttserver, port=8083)


    # ---------------------------------
    # 前端發送Request修改單位電價(0.01~99.99)
    @cherrypy.expose
    def unit_request(self, PMID, PRICE, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            PRICE = str(int(float(PRICE) * 100)).zfill(4)
            case = 'u'
            actionNo = "U"+datetime.now(tz).strftime('%Y%m%d%H%M')
            nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
            pmdb.db_function("db_actionnote", PMID, nt, actionNo, PRICE)
            scheduler.add_job(mqtt_err_job, 'interval', seconds=0.8,
                                id='mqtt_err_job_'+PMID+case, args=[PMID, case, actionNo], replace_existing=True)
            publish.single(PMID, "unit"+PRICE+serverno,
                               hostname=mqttserver, port=8083)
            return actionNo

    # 前端發送Request取得單價修改狀態
    @cherrypy.expose
    def unit_get(self, PMID, No="", KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else:
            result = pmdb.db_function("db_actioncheck", PMID, No)
            return nt+"&"+result

# ---------------------------------
    # 前端發送Request修改扣費模式
    @cherrypy.expose
    def type_request(self, PMID, TYPE, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return "err2"
        else:
            if TYPE == "0":  # 電表扣費
                case = "t"
                scheduler.add_job(mqtt_err_job, 'interval', seconds=timelong,
                                    id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
                publish.single(PMID, "type"+serverno,
                                   hostname=mqttserver, port=8083)
            else:  # 讀卡機扣費
                case = "p"
                scheduler.add_job(mqtt_err_job, 'interval', seconds=timelong,
                                    id='mqtt_err_job_'+PMID+case, args=[PMID, case], replace_existing=True)
                publish.single(PMID, case+serverno,
                                   hostname=mqttserver, port=8083)

    # 前端發送Request取得扣費模式
    @cherrypy.expose
    def type_get(self, PMID, KEY=""):
        nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        result = pmdb.db_function("db_idcheck", PMID)
        if not result: return nt+"&err2"
        else: return nt + "&Success"

# ---------------------------------
    # 重新 Subscribe MQTT
    @cherrypy.expose
    def mqtt_subscribe_reset(self):
        auth = cherrypy.request.headers.get('Authorization', '')
        if auth != 'Bearer miezo53773481':
            cherrypy.response.status = 401
            return 'Unauthorized'
        do_subscribe()
        return 'ok'

    # 取得設備連線狀態（僅支援客戶API串接）
    @cherrypy.expose
    def get_conn_state(self, PMID, KEY):
        if pmdb.db_function("db_keycheck", KEY) != True:
            return "err1"   
        else:
            result = pmdb.db_function("db_get_conn_state", PMID)
            return result
        

cherrypy.server.socket_port = 443
cherrypy.server.socket_host = '0.0.0.0'
cherrypy.server.ssl_module = 'builtin'
cherrypy.server.ssl_certificate = "cert.pem"
cherrypy.server.ssl_private_key = "privkey.pem"



def force_tls():
    if 'X-Forwarded-Proto' in cherrypy.request.headers:
        if cherrypy.request.headers['X-Forwarded-Proto'] == "http":
            raise cherrypy.HTTPRedirect(
                cherrypy.url().replace("http:", "https:"), status=301)

def load_http_server():
    server = cherrypy._cpserver.Server()
    server.socket_host = "0.0.0.0"
    server.socket_port = 80
    server.subscribe()
    
# ----------------------------- [ 系統設定 ] -----------------------------
# 設定MQTT連線
client.on_log=on_log
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.reconnect_delay_set(min_delay=1, max_delay=2000)
client.connect(mqttserver, 8083, 60)
client.loop_start()
# client.loop_forever()

cherrypy.tools.force_tls = cherrypy.Tool("before_handler", force_tls)
load_http_server()
cherrypy.lib.sessions.init(persistent=False)
cherrypy.quickstart(Root(), config=con.conf)


