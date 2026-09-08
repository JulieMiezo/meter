#!/usr/bin/env python3
"""
測試數據庫連接池是否正常運作
"""
import pm_db
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

load_dotenv()

def test_single_connection():
    """測試單個連接"""
    result = pm_db.db_function("db_mqttsub")
    print(f"✓ 單個連接成功，查詢到 {len(result)} 個電表")
    return result

def test_concurrent_connections():
    """測試併發連接"""
    print("\n測試 10 個並發連接...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(test_single_connection) for _ in range(10)]
        results = []
        for i, future in enumerate(as_completed(futures)):
            try:
                results.append(future.result())
                print(f"  連接 {i+1}/10 成功")
            except Exception as e:
                print(f"  ❌ 連接 {i+1}/10 失敗: {e}")
    return len(results) == 10

def test_rapid_queries():
    """測試快速連續查詢"""
    print("\n測試 50 個快速連續查詢...")
    success = 0
    for i in range(50):
        try:
            result = pm_db.db_function("db_mqttsub")
            success += 1
            if (i + 1) % 10 == 0:
                print(f"  {i+1}/50 查詢成功")
        except Exception as e:
            print(f"  ❌ 查詢 {i+1} 失敗: {e}")
    print(f"成功率: {success}/50")
    return success == 50

if __name__ == "__main__":
    print("=" * 50)
    print("數據庫連接池測試")
    print("=" * 50)

    try:
        # 初始化連接池
        pm_db.init_db_pool()
        print("✓ 連接池初始化成功")

        # 測試單個連接
        print("\n[測試 1] 單個連接")
        result1 = test_single_connection()

        # 測試並發連接
        print("\n[測試 2] 並發連接")
        result2 = test_concurrent_connections()

        # 測試快速查詢
        print("\n[測試 3] 快速連續查詢")
        result3 = test_rapid_queries()

        # 總結
        print("\n" + "=" * 50)
        print("測試結果總結:")
        print(f"  單個連接: {'✓ 通過' if result1 else '❌ 失敗'}")
        print(f"  並發連接: {'✓ 通過' if result2 else '❌ 失敗'}")
        print(f"  快速查詢: {'✓ 通過' if result3 else '❌ 失敗'}")

        if result1 and result2 and result3:
            print("\n🎉 所有測試通過！連接池運作正常")
        else:
            print("\n⚠️  部分測試失敗，請檢查配置")

    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()
