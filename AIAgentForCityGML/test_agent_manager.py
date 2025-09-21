#!/usr/bin/env python3
"""
AgentManagerのテストスクリプト
PostgreSQLツールの統合とエラーハンドリングをテスト
"""

import os
import json
import logging
from agent_manager import AgentManager

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_agent_manager():
    """AgentManagerのテスト"""
    try:
        # AgentManagerを初期化
        logger.info("AgentManagerを初期化中...")
        agent_manager = AgentManager([])
        logger.info("AgentManagerの初期化が完了しました")
        
        # 利用可能なプラグインを表示
        logger.info(f"読み込まれたプラグイン数: {len(agent_manager._agent.tools)}")
        for tool in agent_manager._agent.tools:
            logger.info(f"  - {tool.name}: {tool.description}")
        
        return agent_manager
        
    except Exception as e:
        logger.error(f"AgentManagerの初期化に失敗しました: {e}")
        return None

def test_postgres_queries(agent_manager):
    """PostgreSQLクエリのテスト"""
    if not agent_manager:
        logger.error("AgentManagerが初期化されていません")
        return
    
    # テストクエリのリスト
    test_queries = [
        "大阪市内の建物用途別件数を集計して",
        "建物の高さ別分布を分析して",
        "災害リスクの高い建物を用途別に集計して",
        "地理的範囲を指定して建物データを分析して"
    ]
    
    for i, query in enumerate(test_queries, 1):
        logger.info(f"\n=== テストクエリ {i}: {query} ===")
        try:
            result = agent_manager.query(query)
            logger.info(f"結果: {result}")
        except Exception as e:
            logger.error(f"クエリの実行に失敗しました: {e}")
            # エラーハンドリングのテスト
            logger.info("エラーハンドリング機能をテスト中...")
            try:
                error_result = agent_manager._handle_query_error(query, str(e))
                logger.info(f"エラー修正結果: {error_result}")
            except Exception as retry_error:
                logger.error(f"エラー修正も失敗しました: {retry_error}")

def test_error_handling(agent_manager):
    """エラーハンドリングのテスト"""
    if not agent_manager:
        logger.error("AgentManagerが初期化されていません")
        return
    
    logger.info("\n=== エラーハンドリングテスト ===")
    
    # 意図的にエラーを発生させるクエリ
    error_queries = [
        "存在しないテーブルからデータを取得して",  # テーブルエラー
        "不正なSQLクエリを実行して",  # SQLエラー
        "データベース接続エラーを発生させて"  # 接続エラー
    ]
    
    for i, query in enumerate(error_queries, 1):
        logger.info(f"\n--- エラーテスト {i}: {query} ---")
        try:
            result = agent_manager.query(query)
            logger.info(f"結果: {result}")
        except Exception as e:
            logger.info(f"期待通りエラーが発生しました: {e}")
            # エラーハンドリングの動作確認
            try:
                error_result = agent_manager._handle_query_error(query, str(e))
                logger.info(f"エラー修正の試行結果: {error_result}")
            except Exception as retry_error:
                logger.info(f"エラー修正も失敗（期待通り）: {retry_error}")

def main():
    """メイン関数"""
    logger.info("=== AgentManager テスト開始 ===")
    
    # 環境変数の確認
    required_env = ["OPEN_AI_API_KEY"]
    missing_env = [env for env in required_env if not os.getenv(env)]
    if missing_env:
        logger.warning(f"環境変数が設定されていません: {missing_env}")
        logger.info("テストは続行しますが、LLM機能は動作しません")
    
    # PostgreSQL環境変数の確認
    postgres_env = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing_postgres = [env for env in postgres_env if not os.getenv(env)]
    if missing_postgres:
        logger.warning(f"PostgreSQL環境変数が設定されていません: {missing_postgres}")
        logger.info("PostgreSQLツールは動作しません")
    
    # AgentManagerのテスト
    agent_manager = test_agent_manager()
    
    if agent_manager:
        # PostgreSQLクエリのテスト
        test_postgres_queries(agent_manager)
        
        # エラーハンドリングのテスト
        test_error_handling(agent_manager)
    
    logger.info("=== テスト完了 ===")

if __name__ == "__main__":
    main()
