#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Yc-Ma
Desc: MCP WX ChatInsight - A Model Context Protocol server for WeChat data analysis
Time: 2025-05-04 19:00:00
"""

from . import server
import asyncio
import argparse
import json
from typing import Union, List
import sys
import logging

def setup_debug_logging():
    """设置调试日志配置。"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def validate_db_names(db_names: Union[str, List[str]], mode: str, logger=None) -> Union[str, List[str]]:
    """根据模式验证数据库名称。"""
    if logger:
        logger.debug(f"正在验证数据库名称: {db_names} 在模式: {mode}")
    
    if mode == 'cross_db':
        if isinstance(db_names, str):
            # 按逗号分割并去除空白
            db_names = [name.strip() for name in db_names.split(',')]
        elif isinstance(db_names, list):
            db_names = [str(name).strip() for name in db_names]
        else:
            raise ValueError("在跨库模式下，--db 必须是逗号分隔的字符串或数据库名称列表")
        
        if not all(name for name in db_names):
            raise ValueError("不允许空的数据库名称")
        if len(db_names) < 2:
            raise ValueError("跨库模式至少需要2个数据库名称")
    else:  # 单库模式
        if not isinstance(db_names, str):
            raise ValueError("在单库模式下，--db 必须是单个数据库名称字符串")
    
    if logger:
        logger.debug(f"验证成功。最终数据库名称: {db_names}")
    return db_names

def validate_table_name(table_name: str, logger=None) -> str:
    """验证表名。"""
    if logger:
        logger.debug(f"正在验证表名: {table_name}")
    
    if not isinstance(table_name, str):
        raise ValueError("表名必须是字符串")
    if not table_name.strip():
        raise ValueError("表名不能为空")
    
    result = table_name.strip()
    if logger:
        logger.debug(f"验证成功。最终表名: {result}")
    return result

def main():
    """包的主入口点。"""
    parser = argparse.ArgumentParser(
        description='MCP WX ChatInsight - 一个用于微信数据分析的模型上下文协议服务器',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['cross_db', 'single_db'],
        default='single_db',
        help='数据库模式: cross_db (跨库) 或 single_db (不跨库)'
    )
    
    parser.add_argument(
        '--db',
        type=str,
        required=True,
        help='数据库名称。在跨库模式下，以JSON数组形式提供 ["db1", "db2"]。在单库模式下，以字符串形式提供 "db1"'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        required=True,
        help='要分析的表名（例如：wx_record）'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式，显示详细日志'
    )

    parser.add_argument(
        '--transport',
        type=str,
        choices=['stdio', 'sse'],
        default='stdio',
        help='传输模式: stdio 或 sse'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='SSE模式的端口号（默认：8000）'
    )

    parser.add_argument(
        '--sse_path',
        type=str,
        default='/mcp-wx-chatinsight/sse',
        help='SSE端点路径（默认：/mcp-wx-chatinsight/sse）'
    )

    try:
        args = parser.parse_args()
        
        # 如果启用调试模式，设置日志
        logger = setup_debug_logging() if args.debug else None
        if logger:
            logger.debug("已启用调试模式")
            logger.debug(f"参数: mode={args.mode}, db={args.db}, table={args.table}, transport={args.transport}, port={args.port}, sse_path={args.sse_path}")
        
        # 根据模式验证数据库名称
        db_names = validate_db_names(args.db, args.mode, logger)
        
        # 验证表名
        table_name = validate_table_name(args.table, logger)
        
        # 使用SSE模式时验证端口和sse_path
        if args.transport == 'sse':
            if not 1024 <= args.port <= 65535:
                raise ValueError("端口必须在1024到65535之间")
            if not args.sse_path.startswith('/'):
                raise ValueError("SSE路径必须以'/'开头")
            if not args.sse_path.endswith('/sse'):
                raise ValueError("SSE路径必须以'/sse'结尾")
        
        if logger:
            logger.debug("正在使用以下配置启动服务器:")
            logger.debug(f"模式: {args.mode}")
            logger.debug(f"数据库: {db_names}")
            logger.debug(f"表: {table_name}")
            logger.debug(f"传输: {args.transport}")
            if args.transport == 'sse':
                logger.debug(f"端口: {args.port}")
                logger.debug(f"SSE路径: {args.sse_path}")
        
        # 使用验证后的配置运行服务器
        asyncio.run(server.main(
            mode=args.mode,
            db_names=db_names,
            table_name=table_name,
            transport=args.transport,
            port=args.port,
            sse_path=args.sse_path
        ))
        
    except ValueError as e:
        error_msg = f"错误: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_msg = f"意外错误: {str(e)}"
        if logger:
            logger.error(error_msg, exc_info=True)
        print(error_msg, file=sys.stderr)
        sys.exit(1)

# 可选：在包级别暴露其他重要项
__all__ = ['main', 'server']

