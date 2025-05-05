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
from mcp_wx_chatinsight.report import report_generate

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
    def __init__(self, config: DatabaseConfig, table_names: List[str]):
        self.config = config
        self.pools: Dict[str, aiomysql.Pool] = {}
        self.insights: list[str] = []
        self.table_names = table_names
        self.ddl_file = 'ddl.txt'

    async def get_pool(self, table_name: str) -> aiomysql.Pool:
        if '.' in table_name:
            db_name = table_name.split('.')[0]
        else:
            raise ValueError(f"表名格式错误，请使用 db_name.table_name 格式")
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
    
    async def describe_table(self) -> str:
        """获取数据表的详细表结构（DDL）信息。"""
        columns = []
        # 获取任意一个表的结构信息即可
        pool = await self.get_pool(self.table_names[0])  # 使用第一个数据库
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"SHOW CREATE TABLE {self.table_names[0]}")
                db_columns = await cur.fetchall()
                columns.extend(db_columns)
        if len(self.table_names) > 1:
            text = ",".join(self.table_names)
            prefix = f"表 {text} 的结构信息如下（{len(self.table_names)}个表DDL是相同的）：\n"
        else:
            prefix = f"表 {self.table_names[0]} 的结构信息如下：\n"
        return prefix + "".join(f"{column['Create Table']}\n" for column in columns)
    
    async def ddl(self) -> str:
        """获取数据表的详细表结构（DDL）信息。"""
        return await self.describe_table()
    
    def read_ddl(self) -> str:
        """读取DDL文件内容（使用文件读取DDL，避免同步异步操作的冲突）。"""
        with open(self.ddl_file, 'r') as f:
            return f.read()


class ChatInsightServer:
    def __init__(self, table_names: List[str], desc: str = "", sse_path: str = "/mcp-wx-chatinsight/sse"):
        self.table_names = table_names # [db1.table,db2.table...] 格式
        self.desc = desc
        self.db_config = DatabaseConfig()
        self.db_manager = DatabaseManager(self.db_config,self.table_names)
        self._tool_query_desc = self._tool_query_description()
        
        self.mcp = FastMCP(name="mcp-wx-chatinsight", sse_path=sse_path)
        self._register_tools()
    
    def _tool_query_description(self) -> str:
        """提供给`query`工具的描述信息"""
        if len(self.table_names) > 1:
            return f"""
                这些表存储的主要数据内容是{self.desc}。
            Args:
                query (str): SQL SELECT 查询语句。格式要求：
                    - 必须以 SELECT 开头，表名统一使用 db_name.table_name 格式
                    - 支持多库多表查询：多个库表时表示数据是使用分库分表技术存储的，查询时需要合并（使用 UNION ALL 连接）数据
                    - 数据量限制：单次查询数据量可限制为10000条，也可以要求用户分批次查询或者增加过滤条件（例如：`SELECT DISTINCT NAME FROM db1.wx_record LIMIT 10000 UNION ALL SELECT DISTINCT NAME FROM db2.wx_record LIMIT 10000;`）
                    - 表结构信息：{self.db_manager.read_ddl()}
            """
        else:
            return f"""
                这些表存储的主要数据内容是{self.desc}。
            Args:
                query (str): SQL SELECT 查询语句。格式要求：
                    - 必须以 SELECT 开头，表名统一使用 db_name.table_name 格式
                    - 数据量限制：单次查询数据量可限制为10000条，也可以要求用户分批次查询或者增加过滤条件（例如：`SELECT DISTINCT NAME FROM db1.wx_record LIMIT 10000;`）
                    - 表结构信息：{self.db_manager.read_ddl()}
            """

    def _register_tools(self):
        
        @self.mcp.resource(uri='memo://business_insights', name='业务洞察备忘录', description='记录已发现的业务洞察的动态文档', mime_type='text/plain')
        async def resource() -> str:
            return await self.db_manager._synthesize_memo()

        @self.mcp.prompt(name='chatinsight',
                         description='一个提示，用于初始化数据库并演示如何使用 MySQL MCP 服务器 + LLM')
        async def prompt(topic: str = Field(description="用于初始化数据库的初始数据主题",required=True)) -> types.GetPromptResult:
            logger.debug(f"处理获取提示请求，主题: {topic}")
            prompt = PROMPT_TEMPLATE.format(topic=topic)

            logger.debug(f"为主题: {topic} 生成了提示模板")
            return types.GetPromptResult(
                description=f"用于 {topic} 的示例模板",
                messages=[
                types.PromptMessage(role="user",content=types.TextContent(type="text", text=prompt.strip()))])
        
        @self.mcp.tool(description=f"""对数据表 {",".join(self.table_names)} 执行 SQL SELECT 查询并返回结果。
                    {self._tool_query_desc}""")
        async def query(query: str) -> List[Dict[str, Any]]:
            if not query.strip().upper().startswith('SELECT'):
                raise ValueError("只允许 SELECT 查询")
            
            try:
                results = []
                pool = await self.db_manager.get_pool(self.table_names[0])
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(query)
                        db_results = await cur.fetchall()
                        results.extend(db_results)
                return results
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                raise
        
        @self.mcp.tool(description=f"""生成自定义时间范围的数据总结报告（日报、周报、月报等）。生成的SQL一般必须包含时间范围的过滤。
                       {self._tool_query_desc}
                       """)
        async def report(query: str) -> str:
            """使用GraphRAG生成数据总结报告"""
            if not query.strip().upper().startswith('SELECT'):
                raise ValueError("只允许 SELECT 查询")
            
            try:
                results = []
                pool = await self.db_manager.get_pool(self.table_names[0])
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(query)
                        db_results = await cur.fetchall()
                        results.extend(db_results)
                report = await report_generate(results,self.desc)
                return report
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                raise

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

            return [types.TextContent(type="text", text="洞察已添加到备忘录")]

    async def start(self, transport: str = "stdio", port: int = 8000):
        """启动服务器。"""
        try:
            # 初始化数据库连接
            for table_name in self.table_names:
                await self.db_manager.get_pool(table_name)
            
            # 写入DDL到文件
            with open(self.db_manager.ddl_file, 'w') as f:
                f.write(await self.db_manager.ddl())
            logger.debug(f"DDL文件已写入 {self.db_manager.ddl_file}: {self.db_manager.read_ddl()}")
            
            # 使用指定的传输方式启动 MCP 服务器
            if transport == "sse":
                await self.mcp.run_async(transport="sse", host="0.0.0.0", port=port)
            else:  # stdio
                await self.mcp.run_async(transport="stdio")
        finally:
            # 清理数据库连接
            await self.db_manager.close()

def main(table_names: List[str], desc: str = "", transport: str = "stdio", port: int = 8000, sse_path: str = "/mcp-wx-chatinsight/sse"):
    """服务器的主入口点。"""
    server = ChatInsightServer(table_names, desc, sse_path)
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
    main(table_names=["test1.wx_record","test2.wx_record"], desc="微信群聊数据")

