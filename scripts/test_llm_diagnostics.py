#!/usr/bin/env python3
"""
LLM 诊断测试脚本

测试项目:
1. 连通性 - 基本 API 调用是否成功
2. 响应时间 - 单次调用延迟
3. 速率限制 - 连续请求测试实际 RPM/TPM
4. 错误恢复 - 观察重试行为
5. 建议配置 - 根据测试结果输出优化建议
"""

import os
import sys
import asyncio
import time
import logging
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from src.utils.llm_factory import LLMFactory
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LLM.Diagnostics")

# Test prompt (small, fast)
TEST_PROMPT = "Reply with only the word 'OK'."

# Slightly larger prompt for token measurement
MEDIUM_PROMPT = "Summarize the following in one sentence: The quick brown fox jumps over the lazy dog. This is a test of the language model API."


class DiagnosticResult:
    def __init__(self):
        self.connectivity_ok = False
        self.model_name = ""
        self.base_url = ""
        self.response_times = []
        self.rate_limit_hit = False
        self.rate_limit_at_request = 0
        self.errors = []
        self.total_requests_sent = 0
        self.total_requests_success = 0
        self.burst_window_seconds = 0


async def test_connectivity(result: DiagnosticResult):
    """Test 1: Basic connectivity"""
    print("\n" + "=" * 60)
    print("🔌 Test 1: 连通性测试 (Connectivity)")
    print("=" * 60)
    
    settings = get_settings()
    result.model_name = settings.LLM_MODEL
    result.base_url = settings.OPENAI_API_BASE
    
    print(f"  Model:    {result.model_name}")
    print(f"  Base URL: {result.base_url}")
    
    try:
        llm = LLMFactory.create_llm(temperature=0)
        response = await llm.ainvoke(TEST_PROMPT)
        print(f"  Response: {response.content[:100]}")
        print(f"  ✅ 连通性正常")
        result.connectivity_ok = True
    except Exception as e:
        print(f"  ❌ 连通性失败: {e}")
        result.errors.append(f"Connectivity: {e}")


async def test_response_time(result: DiagnosticResult, rounds: int = 5):
    """Test 2: Response time measurement"""
    print("\n" + "=" * 60)
    print(f"⏱️  Test 2: 响应时间测试 (Response Time, {rounds} rounds)")
    print("=" * 60)
    
    if not result.connectivity_ok:
        print("  ⚠️ 跳过 (连通性测试失败)")
        return
    
    llm = LLMFactory.create_llm(temperature=0)
    
    for i in range(rounds):
        start = time.time()
        try:
            response = await llm.ainvoke(TEST_PROMPT)
            elapsed = time.time() - start
            result.response_times.append(elapsed)
            result.total_requests_success += 1
            print(f"  Round {i+1}: {elapsed:.2f}s  ({response.content[:30].strip()})")
        except Exception as e:
            elapsed = time.time() - start
            print(f"  Round {i+1}: FAILED ({elapsed:.2f}s) - {e}")
            result.errors.append(f"Response time round {i+1}: {e}")
        result.total_requests_sent += 1
        # Small delay between rounds to not trigger rate limit yet
        await asyncio.sleep(1)
    
    if result.response_times:
        avg = sum(result.response_times) / len(result.response_times)
        min_t = min(result.response_times)
        max_t = max(result.response_times)
        print(f"\n  📊 平均: {avg:.2f}s | 最快: {min_t:.2f}s | 最慢: {max_t:.2f}s")


async def test_rate_limit(result: DiagnosticResult, max_requests: int = 30, window_seconds: int = 60):
    """Test 3: Rate limit discovery - send rapid requests without delay"""
    print("\n" + "=" * 60)
    print(f"🚦 Test 3: 速率限制测试 (Rate Limit, max {max_requests} requests in {window_seconds}s)")
    print("=" * 60)
    
    if not result.connectivity_ok:
        print("  ⚠️ 跳过 (连通性测试失败)")
        return
    
    # Create LLM with NO internal retries to see raw rate limit behavior
    from langchain_openai import ChatOpenAI
    settings = get_settings()
    llm = ChatOpenAI(
        model=settings.LLM_MODEL,
        temperature=0,
        base_url=settings.OPENAI_API_BASE,
        api_key=settings.OPENAI_API_KEY,
        max_retries=0,  # No retries - we want to see raw errors
        timeout=30
    )
    
    success_count = 0
    fail_count = 0
    rate_limit_count = 0
    start_time = time.time()
    request_times = []
    
    for i in range(max_requests):
        elapsed_total = time.time() - start_time
        if elapsed_total > window_seconds:
            print(f"\n  ⏰ 时间窗口 ({window_seconds}s) 已到，停止测试")
            break
        
        req_start = time.time()
        try:
            response = await llm.ainvoke(TEST_PROMPT)
            req_time = time.time() - req_start
            success_count += 1
            request_times.append(req_time)
            status = "✅"
        except Exception as e:
            req_time = time.time() - req_start
            error_str = str(e).lower()
            if "rate" in error_str or "429" in error_str or "limit" in error_str or "quota" in error_str or "resource" in error_str:
                rate_limit_count += 1
                if not result.rate_limit_hit:
                    result.rate_limit_hit = True
                    result.rate_limit_at_request = i + 1
                status = "🚫 RATE LIMITED"
            else:
                fail_count += 1
                status = f"❌ {str(e)[:60]}"
        
        result.total_requests_sent += 1
        print(f"  [{i+1:2d}/{max_requests}] {elapsed_total:5.1f}s | {req_time:.2f}s | {status}")
        
        # Minimal delay - just enough to not overwhelm the event loop
        await asyncio.sleep(0.1)
    
    total_time = time.time() - start_time
    result.burst_window_seconds = total_time
    actual_rpm = (success_count / total_time * 60) if total_time > 0 else 0
    
    print(f"\n  📊 结果统计:")
    print(f"     总请求: {success_count + fail_count + rate_limit_count}")
    print(f"     成功:   {success_count}")
    print(f"     失败:   {fail_count}")
    print(f"     限流:   {rate_limit_count}")
    print(f"     耗时:   {total_time:.1f}s")
    print(f"     实际 RPM: ~{actual_rpm:.0f}")
    if result.rate_limit_hit:
        print(f"     首次限流: 第 {result.rate_limit_at_request} 个请求")


async def test_concurrent(result: DiagnosticResult, concurrency: int = 3):
    """Test 4: Concurrent request simulation (production-like)"""
    print("\n" + "=" * 60)
    print(f"🔀 Test 4: 并发测试 (Concurrency = {concurrency})")
    print("=" * 60)
    
    if not result.connectivity_ok:
        print("  ⚠️ 跳过 (连通性测试失败)")
        return
    
    llm = LLMFactory.create_llm(temperature=0)
    
    async def single_call(idx: int):
        start = time.time()
        try:
            response = await llm.ainvoke(MEDIUM_PROMPT)
            elapsed = time.time() - start
            return idx, True, elapsed, response.content[:30]
        except Exception as e:
            elapsed = time.time() - start
            return idx, False, elapsed, str(e)[:50]
    
    tasks = [single_call(i) for i in range(concurrency)]
    start = time.time()
    results_list = await asyncio.gather(*tasks)
    total = time.time() - start
    
    for idx, ok, elapsed, msg in results_list:
        status = "✅" if ok else "❌"
        print(f"  Task {idx}: {status} {elapsed:.2f}s - {msg}")
    
    print(f"\n  📊 总耗时: {total:.2f}s ({concurrency} 个并发请求)")


def print_recommendations(result: DiagnosticResult):
    """Print optimization recommendations based on test results"""
    print("\n" + "=" * 60)
    print("📋 优化建议 (Recommendations)")
    print("=" * 60)
    
    if not result.connectivity_ok:
        print("  ❌ 无法给出建议 - LLM 连接失败，请检查:")
        print(f"     - OPENAI_API_BASE: {result.base_url}")
        print(f"     - LLM_MODEL: {result.model_name}")
        print("     - 网络连接和 API Key")
        return
    
    # RPM recommendation
    if result.rate_limit_hit:
        safe_rpm = max(1, result.rate_limit_at_request - 2)
        print(f"\n  🚦 速率限制:")
        print(f"     在第 {result.rate_limit_at_request} 个请求时触发限流")
        print(f"     建议 LLM_MAX_RPM: {safe_rpm} (留 2 个请求余量)")
    else:
        print(f"\n  🚦 速率限制:")
        print(f"     在 {result.total_requests_sent} 个请求中未触发限流")
        print(f"     建议 LLM_MAX_RPM: 30 (当前设置过于保守)")
    
    # Response time analysis
    if result.response_times:
        avg = sum(result.response_times) / len(result.response_times)
        max_t = max(result.response_times)
        
        print(f"\n  ⏱️ 响应时间:")
        print(f"     平均: {avg:.2f}s | 最慢: {max_t:.2f}s")
        
        if avg > 10:
            print(f"     ⚠️ 响应偏慢，建议增加 timeout 到 {int(max_t * 2)}s")
        elif avg > 5:
            print(f"     💡 响应时间中等，当前 timeout=60s 足够")
        else:
            print(f"     ✅ 响应时间良好")
    
    # Retry strategy
    print(f"\n  🔄 重试策略 (当前已优化):")
    print(f"     外层 tenacity: 3 次 (指数退避 max=60s)")
    print(f"     内层 OpenAI SDK: 2 次")
    print(f"     最坏情况: 3×2=6 次调用, 约 2-3 分钟超时")
    
    if result.errors:
        print(f"\n  ⚠️ 测试中遇到的错误:")
        for err in result.errors[:5]:
            print(f"     - {err}")


async def main():
    print("🔬 AI Email Assistant - LLM 诊断工具")
    print("=" * 60)
    
    result = DiagnosticResult()
    
    await test_connectivity(result)
    await test_response_time(result)
    await test_rate_limit(result)
    await test_concurrent(result)
    
    print_recommendations(result)
    
    print("\n" + "=" * 60)
    print("✅ 诊断完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
