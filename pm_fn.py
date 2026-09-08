# ------------ // 系統寄出Email
# import qrmeter_db as qrdb
import smtplib
from email.mime.multipart import MIMEMultipart  # email內容載體
from email.mime.text import MIMEText  # 用於製作文字內文
from email.mime.base import MIMEBase  # 用於承載附檔
from email import encoders  # 用於附檔編碼

from datetime import datetime, timezone, timedelta
import pytz
import time
import os
from dotenv import load_dotenv
load_dotenv()
tz = pytz.timezone('Asia/Taipei')


# 寄件者使用的Gmail帳戶資訊
gmail_user = 'qrlockrd@gmail.com'
gmail_password = os.getenv("GMAIL_PASSWORD")
from_address = gmail_user
# texthead = "親愛的客戶 您好：\n"
texthead = ""
textend = "\n※ 本郵件由系統自動發送，請勿直接回覆，如有疑問請聯絡客服人員，我們將儘速為您服務，謝謝您的配合。"


def sendemail(QRID, action, x="", y="", z="", a="", b=""):
    # 設定信件內容與收件人資訊
    to_address = ['rd@miezo.com.tw']
    Subject = "Send from Power Meter Server"
    # METER = qrdb.db_function("db_getmetername", QRID)  # 代入meterID
    # OWNMAIL = qrdb.db_function("db_getownermail", QRID)  # 代入meterID
    nt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    if action == "balalert":
        # to_address = [OWNMAIL, x]
        contents = """
電表『{0}』餘額警示：
目前剩餘金額為{1}元，請盡快加值
        """ .format(x, str(int(y)/100))

    # if to_address == [None] or to_address == [None, None]:
        # to_address = []
    # 開始組合信件內容
    mail = MIMEMultipart()
    mail['From'] = from_address
    mail['To'] = ', '.join(to_address)
    mail['Subject'] = Subject
    # 將信件內文加到email中
    mail.attach(MIMEText(texthead+contents+textend))

    # 設定smtp伺服器並寄發信件
    smtpserver = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    smtpserver.ehlo()
    smtpserver.login(gmail_user, gmail_password)
    smtpserver.sendmail(from_address, to_address +
                        ['julie@miezo.com.tw'], mail.as_string())
    smtpserver.quit()
