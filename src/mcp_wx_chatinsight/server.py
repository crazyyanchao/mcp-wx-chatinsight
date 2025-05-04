#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Yc-Ma
Desc: Server implementation for MCP WX ChatInsight
Time: 2024-03-21 00:00:00
"""

import os
import sys
import logging
from typing import Union, List, Dict, Any
import aiomysql
from fastmcp import FastMCP
import mcp.types as types
from pydantic import AnyUrl, Field
import asyncio
import pdb

from mcp_wx_chatinsight.prompt import PROMPT_TEMPLATE

# 重新配置 Windows 系统下的默认编码（从 windows-1252 改为 utf-8）
if sys.platform == "win32" and os.environ.get('PYTHONIOENCODING') is None:
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logger = logging.getLogger('mcp_wx_chatinsight')
logger.info("Starting MCP WX ChatInsight Server")

class DatabaseConfig:
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST', 'localhost')
        self.port = int(os.getenv('MYSQL_PORT', '3306'))
        self.user = os.getenv('MYSQL_USER', 'root')
        self.password = os.getenv('MYSQL_PASSWORD', '')
        self.default_db = os.getenv('MYSQL_DATABASE', '')

class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pools: Dict[str, aiomysql.Pool] = {}
        self.insights: list[str] = []

    async def get_pool(self, db_name: str) -> aiomysql.Pool:
        if ',' in db_name:
            db_names = db_name.split(',')
        else:
            db_names = [db_name]
        for db_name in db_names:
            if db_name not in self.pools:
                self.pools[db_name] = await aiomysql.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                db=db_name,
                autocommit=True
            )
        return self.pools[db_name]

    async def close(self):
        for pool in self.pools.values():
            pool.close()
            await pool.wait_closed()
    
    async def _synthesize_memo(self) -> str:
        """将业务洞察合成为格式化的备忘录"""
        logger.debug(f"正在合成包含 {len(self.insights)} 条洞察的备忘录")
        if not self.insights:
            return "目前尚未发现任何业务洞察。"

        insights = "\n".join(f"- {insight}" for insight in self.insights)

        memo = "📊 业务洞察备忘录 📊\n\n"
        memo += "关键洞察发现：\n\n"
        memo += insights

        if len(self.insights) > 1:
            memo += "\n总结：\n"
            memo += f"分析揭示了{len(self.insights)}个关键业务洞察，这些洞察为业务战略优化和增长提供了机会。"

        logger.debug("已生成基础备忘录格式")
        return memo

class ChatInsightServer:
    def __init__(self, mode: str, db_names: Union[str, List[str]], table_name: str, sse_path: str = "/mcp-wx-chatinsight/sse"):
        self.mode = mode
        self.db_names = [db_names] if isinstance(db_names, str) else db_names
        self.table_name = table_name
        self.db_config = DatabaseConfig()
        self.db_manager = DatabaseManager(self.db_config)
        
        self.mcp = FastMCP(name="mcp-wx-chatinsight", sse_path=sse_path)
        
        self._register_tools()

    def _register_tools(self):

        @self.mcp.resource(uri='memo://{path}', name='业务洞察备忘录', description='记录已发现的业务洞察的动态文档', mime_type='text/plain')
        async def resource(path: str) -> str:
            logger.debug(f"处理资源读取请求，URI: memo://{path}")
            if not path or path != "memo_insights":
                logger.error(f"未知的资源路径: {path}")
                raise ValueError(f"未知的资源路径: {path}")

            return await self.db_manager._synthesize_memo()

        @self.mcp.prompt(name='mcp-demo',
                         description='一个提示，用于初始化数据库并演示如何使用 MySQL MCP 服务器 + LLM')
        async def prompt(topic: str = Field(description="用于初始化数据库的初始数据主题",required=True)) -> types.GetPromptResult:
            logger.debug(f"处理获取提示请求，主题: {topic}")
            prompt = PROMPT_TEMPLATE.format(topic=topic)

            logger.debug(f"为主题: {topic} 生成了提示模板")
            return types.GetPromptResult(
                description=f"用于 {topic} 的示例模板",
                messages=[
                types.PromptMessage(role="user",content=types.TextContent(type="text", text=prompt.strip()))])

        @self.mcp.tool()
        async def read_query(query: str) -> List[Dict[str, Any]]:
            """执行 SQL SELECT 查询并返回结果。

            Args:
                query (str): SQL SELECT 查询语句。格式要求：
                    - 必须以 SELECT 开头
                    - 支持多表查询：SELECT column1, column2 FROM db1.table1 WHERE condition
                    - 支持多库查询：使用 UNION 语句合并不同数据库的结果

            Returns:
                List[Dict[str, Any]]: 查询结果列表，每个元素是一个字典，键为列名，值为对应的数据

            Raises:
                ValueError: 当查询语句不是以 SELECT 开头时抛出

            Example:
                >>> query = "SELECT id, name FROM db1.users WHERE age > 18"
                >>> # 返回格式: [{"id": 1, "name": "张三"}, {"id": 2, "name": "李四"}]
            """
            if not query.strip().upper().startswith('SELECT'):
                raise ValueError("只允许 SELECT 查询")
            
            results = []
            for db_name in self.db_names:
                pool = await self.db_manager.get_pool(db_name)
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(query)
                        db_results = await cur.fetchall()
                        results.extend(db_results)
            return results

        @self.mcp.tool()
        async def list_tables() -> List[str]:
            """获取所有配置的数据库中的表列表。

            Returns:
                List[str]: 表名列表，格式为 "db_name.table_name"。
                    例如：["test1.wx_record", "test2.wx_record"]

            Note:
                - 返回的表名包含数据库名作为前缀
                - 如果配置了多个数据库，会返回所有数据库的表
            """
            tables = set()
            for db_name in self.db_names:
                pool = await self.db_manager.get_pool(db_name)
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SHOW TABLES")
                        db_tables = await cur.fetchall()
                        tables.update(db_name+'.'+table[0] for table in db_tables)
            return list(tables)

        @self.mcp.tool()
        async def describe_table() -> str:
            """获取数据表的详细表结构（DDL）信息。"""
            columns = []
            # 获取任意一个表的结构信息即可
            pool = await self.db_manager.get_pool(self.db_names[0])
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(f"SHOW CREATE TABLE {self.table_name}")
                    db_columns = await cur.fetchall()
                    columns.extend(db_columns)
            tables = [db_name+'.'+self.table_name for db_name in self.db_names]
            prefix = f"表 {tables} 的结构信息如下（{len(tables)}个表DDL是相同的）：\n"
            return prefix + "".join(f"{column['Create Table']}\n" for column in columns)

        @self.mcp.tool()
        async def append_insight(insight: str) -> Dict[str, str]:
            """添加新的业务洞察记录到系统中。

            该工具用于在数据分析过程中收集和记录重要的业务发现，帮助用户更好地理解和利用数据中的洞察。
            添加的洞察会被自动整合到系统备忘录中，并实时通知客户端更新。

            Args:
                insight (str): 业务洞察内容，应包含对数据的分析和见解。例如："本月用户活跃度较上月提升20%"
            """
            # 添加洞察的实现
            self.db_manager.insights.append(insight)
            await self.db_manager._synthesize_memo()

            # 通知客户端备忘录资源已更新
            await self.mcp.get_context().session.send_resource_updated(AnyUrl("memo://insights"))
            # notice_resource = TextResource(
            #     uri=AnyUrl("memo://insights"),
            #     name="Business Insights Memo",
            #     description="A living document of discovered business insights",
            #     mimeType="text/plain",
            # )
            # self.mcp.add_resource(notice_resource)

            return [types.TextContent(type="text", text="洞察已添加到备忘录")]

    async def start(self, transport: str = "stdio", port: int = 8000):
        """启动服务器。"""
        try:
            # 初始化数据库连接
            for db_name in self.db_names:
                await self.db_manager.get_pool(db_name)
            
            # 使用指定的传输方式启动 MCP 服务器
            if transport == "sse":
                await self.mcp.run_async(transport="sse", host="0.0.0.0", port=port)
            else:  # stdio
                await self.mcp.run_async(transport="stdio")
        finally:
            # 清理数据库连接
            await self.db_manager.close()

def main(mode: str, db_names: Union[str, List[str]], table_name: str, transport: str = "stdio", port: int = 8000, sse_path: str = "/mcp-wx-chatinsight/sse"):
    """服务器的主入口点。"""
    server = ChatInsightServer(mode, db_names, table_name, sse_path)
    asyncio.run(server.start(transport, port))

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="MCP WX ChatInsight")
    # parser.add_argument("--port", type=int, default=8000, help="端口")
    # args = parser.parse_args()
    # main(args)
    # mcp.run(transport="sse", host="0.0.0.0", port=8000) # http://localhost:8000/mcp-wx-chatinsight/sse # sse_path="/mcp-wx-chatinsight/sse"
    # mcp.run(transport="stdio")
    os.environ["MYSQL_HOST"] = "localhost"
    os.environ["MYSQL_PORT"] = "3306"
    os.environ["MYSQL_USER"] = "root"
    os.environ["MYSQL_PASSWORD"] = "123456"
    main(mode="cross_db", db_names="test1,test2", table_name="wx_record")


