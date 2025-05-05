#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Yc-Ma
Desc: Report generation for MCP WX ChatInsight
Time: 2025-05-05 00:00:00
"""

import os
import asyncio
import numpy as np
from typing import Any, Dict, List
from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import gpt_4o_mini_complete, gpt_4o_complete, create_openai_async_client,openai_complete_if_cache
from lightrag.llm.ollama import ollama_embed,ollama_model_complete
from lightrag.kg.shared_storage import initialize_pipeline_status
from lightrag.utils import setup_logger
from lightrag.types import GPTKeywordExtractionFormat
from lightrag.utils import (
    wrap_embedding_func_with_attrs,
    logger,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from openai import (
    AsyncOpenAI,
    APIConnectionError,
    RateLimitError,
    APITimeoutError,
)
from lightrag.utils import EmbeddingFunc

setup_logger("lightrag", level="INFO")

# os.environ["OPENAI_API_BASE"]="http://localhost:11434/v1" # ollama
# os.environ["OPENAI_API_KEY"]="Bearer sh-k-sk-proj"
# MODEL_NAME = "deepseek-r1:latest"

# EMBED_MODEL_URL = "http://localhost:11434/api/embeddings" # ollama
# EMBED_MODEL_NAME = "bge-m3"
# EMBED_MODEL_KEY = "Bearer sh-k-sk-proj"

WORKING_DIR = "./rag_storage"
if not os.path.exists(WORKING_DIR):
    os.mkdir(WORKING_DIR)


# @wrap_embedding_func_with_attrs(embedding_dim=1536, max_token_size=8192)
# @retry(
#     stop=stop_after_attempt(3),
#     wait=wait_exponential(multiplier=1, min=4, max=60),
#     retry=(
#         retry_if_exception_type(RateLimitError)
#         | retry_if_exception_type(APIConnectionError)
#         | retry_if_exception_type(APITimeoutError)
#     ),
# )
# async def openai_embed(
#     texts: list[str],
#     model: str = EMBED_MODEL_NAME,
#     base_url: str = EMBED_MODEL_URL,
#     api_key: str = EMBED_MODEL_KEY,
#     client_configs: dict[str, Any] = None,
# ) -> np.ndarray:
#     """Generate embeddings for a list of texts using OpenAI's API.

#     Args:
#         texts: List of texts to embed.
#         model: The OpenAI embedding model to use.
#         base_url: Optional base URL for the OpenAI API.
#         api_key: Optional OpenAI API key. If None, uses the OPENAI_API_KEY environment variable.
#         client_configs: Additional configuration options for the AsyncOpenAI client.
#             These will override any default configurations but will be overridden by
#             explicit parameters (api_key, base_url).

#     Returns:
#         A numpy array of embeddings, one per input text.

#     Raises:
#         APIConnectionError: If there is a connection error with the OpenAI API.
#         RateLimitError: If the OpenAI API rate limit is exceeded.
#         APITimeoutError: If the OpenAI API request times out.
#     """
#     # Create the OpenAI client
#     openai_async_client = create_openai_async_client(
#         api_key=api_key, base_url=base_url, client_configs=client_configs
#     )

#     response = await openai_async_client.embeddings.create(
#         model=model, input=texts, encoding_format="float"
#     )
#     return np.array([dp.embedding for dp in response.data])

# async def gpt_4o_mini_complete(
#     prompt,
#     system_prompt=None,
#     history_messages=None,
#     keyword_extraction=False,
#     **kwargs,
# ) -> str:
#     if history_messages is None:
#         history_messages = []
#     keyword_extraction = kwargs.pop("keyword_extraction", None)
#     if keyword_extraction:
#         kwargs["response_format"] = GPTKeywordExtractionFormat
#     return await openai_complete_if_cache(
#         MODEL_NAME,
#         prompt,
#         system_prompt=system_prompt,
#         history_messages=history_messages,
#         **kwargs,
#     )

# async def initialize_rag():
#     rag = LightRAG(
#         working_dir=WORKING_DIR,
#         embedding_func=ollama_embed,
#         llm_model_func=ollama_complete,
#     )
#     await rag.initialize_storages()
#     await initialize_pipeline_status()
#     return rag

async def initialize_rag() -> LightRAG:
    rag = LightRAG(
        working_dir=WORKING_DIR,

        chunk_token_size=300,
        chunk_overlap_token_size=0,

        llm_model_func=ollama_model_complete,
        llm_model_name="deepseek-r1:latest",
        llm_model_max_async=1,
        llm_model_max_token_size=8192,
        llm_model_kwargs={"host": "http://localhost:11434", "options": {"num_ctx": 8192}},

        embedding_func=EmbeddingFunc(
            embedding_dim=1024,     # bge-large-zh-v1.5
            max_token_size=512,    # bge-large-zh-v1.5
            func=lambda texts: ollama_embed(
                texts, embed_model="bge-m3", host="http://localhost:11434"
            ),
        ),
    )
    await rag.initialize_storages()
    await initialize_pipeline_status()
    return rag

async def main():
    try:
        # Initialize RAG instance
        rag = await initialize_rag()
        documents = [
            # 模拟数据
            "近日，国家发改委发布最新数据显示，一季度我国经济运行开局良好，GDP同比增长5.3%，各项经济指标稳中向好。",
            "北京市教育委员会宣布，将在全市范围内推广'双减'政策新举措，进一步减轻中小学生课业负担。",
            "中国科学院最新研究成果表明，可再生能源在我国能源结构中的占比首次突破25%，为实现'双碳'目标奠定基础。",
            "深圳市政府出台新政策，将在未来三年内投入500亿元支持人工智能产业发展，打造全球领先的AI创新中心。",
            "农业农村部报告指出，今年全国粮食生产形势总体向好，预计夏粮产量将达到1.4万亿斤，实现增产目标。",
            "上海证券交易所今日发布公告，将进一步完善科创板上市制度，优化融资环境，支持科技创新企业发展。",
            "国家卫健委通报，全国医疗服务体系改革取得显著成效，基层医疗机构服务能力显著提升。",
            "杭州亚运会组委会发布消息，赛事筹备工作已进入最后冲刺阶段，各项基础设施建设基本完成。",
            "生态环境部发布2023年全国环境质量报告，显示全国338个地级及以上城市空气质量持续改善。",
            "商务部最新数据显示，今年前两月我国对外贸易总额同比增长7.8%，外贸发展韧性持续显现。"
        ]
        await rag.ainsert(documents)

        # Perform global search
        mode="global"
        result = await rag.aquery(
              "这个数据的主要主题是什么？",
              param=QueryParam(mode=mode)
          )
        print(result)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if rag:
            await rag.finalize_storages()


async def report_generate(data: List[Dict[str, Any]],desc:str,prompt:bool=True) -> str:
    """使用GraphRAG生成数据总结报告
    Args:
        data: 数据列表
        desc: 数据描述
    Returns:
        str: 数据总结报告
    """
    rag = None
    try:
        if prompt:
            return (
            f"数据背景：{desc}\n"
            "请基于上述背景和以下数据，使用`工件工具生成一份结构化的总结报告，内容包括：\n"
            "1. 主要主题和核心内容；\n"
            "2. 关键发现或亮点；\n"
            "3. 存在的问题或风险（如有）；\n"
            "4. 未来趋势或建议（如适用）。\n"
            "请用简明扼要的语言输出，适用于日报、周报或月报场景。"
            f"数据：{data}"
            )
        # Initialize RAG instance
        rag = await initialize_rag()
        # Convert dictionary data to formatted strings
        documents = [str(dat) for dat in data]
        await rag.ainsert(documents)

        query = (
            f"数据背景：{desc}\n"
            "请基于上述背景和以下数据，生成一份结构化的总结报告，内容包括：\n"
            "1. 主要主题和核心内容；\n"
            "2. 关键发现或亮点；\n"
            "3. 存在的问题或风险（如有）；\n"
            "4. 未来趋势或建议（如适用）。\n"
            "请用简明扼要的语言输出，适用于日报、周报或月报场景。"
        )
        result = await rag.aquery(query,param=QueryParam(mode="global"))
        logger.info(f"Report generated: {result}")
        return result
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return f"报告生成失败: {e}"
    finally:
        if rag is not None:
            await rag.finalize_storages()

if __name__ == "__main__":
    print(asyncio.run(main()))

