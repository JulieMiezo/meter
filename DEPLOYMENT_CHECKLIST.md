# 快速部署清單

## 前置檢查
- [ ] 已讀過 DB_LOCK_FIX.md 文檔
- [ ] 原始 pm_db.py 已備份至 pm_db.py.backup
- [ ] 新的 pm_db.py 已替換
- [ ] server.py 已更新（添加消息隊列）

## 部署步驟

### 1️⃣ 環境準備
```bash
# 進入專案目錄
cd /Users/chiaoyuhsiao/Desktop/[ 軟體 ]/=電表=/V2_整合\ qrmeter

# 升級 mysql-connector-python
pip install --upgrade mysql-connector-python

# 驗證版本 >= 8.0.30
python3 -c "import mysql.connector; print(mysql.connector.__version__)"
```

### 2️⃣ 測試連接池
```bash
# 執行連接池測試
python3 test_db_pool.py

# 預期輸出：
# ✓ 連接池初始化成功
# ✓ 單個連接成功，查詢到 N 個電表
# ✓ 10 個並發連接都成功
# 🎉 所有測試通過！連接池運作正常
```

### 3️⃣ 停止舊應用
```bash
# 如果應用正在運行，停止它
# Ctrl+C (if running in foreground)
# 或
# killall python3  # 謹慎使用

# 驗證已停止
ps aux | grep server.py
```

### 4️⃣ 啟動新應用
```bash
# 啟動新版本
python3 server.py

# 預期看到的日誌：
# [時間] INFO: Database worker thread started
# [時間] MQTT connected successfully
```

### 5️⃣ 驗證功能
```bash
# 監控日誌（新終端窗口）
tail -f nohup.out  # 如果使用 nohup
# 或
tail -f your_log_file.log

# 觀察是否有以下訊息：
# ✓ Database worker thread started
# ✓ MQTT connected successfully
# ✓ MQTT subscribed: N meters
```

## 異常情況

### 問題：ImportError: No module named 'mysql.connector'
**解決：**
```bash
pip install --upgrade mysql-connector-python
```

### 問題：AttributeError: pooling module not found
**解決：**
```bash
# 確保安裝的是最新版本
pip install --upgrade --force-reinstall mysql-connector-python
```

### 問題：仍然看到鎖定超時錯誤
**檢查步驟：**
1. 確認日誌中看到重試訊息：
   ```
   WARNING: [case_name] 鎖定超時，X 秒後重試 (Y/3)
   ```

2. 如果沒有重試訊息，檢查是否使用了舊的 pm_db.py：
   ```bash
   grep "MAX_RETRIES" pm_db.py
   # 應該找到 MAX_RETRIES = 3
   ```

3. 如果重試 3 次後仍失敗，可能是其他應用佔用連接
   ```sql
   SELECT USER, COUNT(*) as cnt FROM INFORMATION_SCHEMA.PROCESSLIST GROUP BY USER;
   ```

## 回滾計畫

如果需要回到舊版本：
```bash
# 恢復備份
cp pm_db.py.backup pm_db.py

# 重啟應用
python3 server.py
```

## 文件清單

| 文件 | 用途 | 是否已部署 |
|-----|------|-----------|
| pm_db.py | 新的連接池實現 | ✅ |
| pm_db.py.backup | 備份 | ✅ |
| server.py | 更新消息隊列 | ✅ |
| DB_LOCK_FIX.md | 詳細文檔 | ✅ |
| test_db_pool.py | 測試腳本 | ✅ |
| DEPLOYMENT_CHECKLIST.md | 本文件 | ✅ |

## 預期結果

部署完成後應該看到：
- ✅ 應用正常啟動
- ✅ MQTT 連接成功
- ✅ 偶爾看到重試日誌（正常現象）
- ✅ 不再看到「連接失敗」直接返回 err 的情況

## 效能改進指標

可以在應用穩定運行一周後檢查：
- 連接失敗率是否降低 90%+ ？
- 平均響應時間是否縮短 50%+ ？
- MQTT 消息是否都被正確處理？

## 支援聯繫

如有問題：
1. 檢查 DB_LOCK_FIX.md 中的故障排查部分
2. 查看日誌中的重試訊息
3. 運行 test_db_pool.py 驗證連接池狀態
