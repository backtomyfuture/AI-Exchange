FROM python:3.12-slim

# 创建非 root 用户
RUN useradd -m -u 1000 appuser

WORKDIR /app

# 安装必要的系统依赖（使用默认源以避免特定网络环境下的签名校验错误）
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libjpeg-dev \
    libopenjp2-7-dev \
    libffi-dev \
    curl \
    fonts-noto-cjk \
    fontconfig \
    && rm -rf /var/lib/apt/lists/*


# 复制锁定的依赖清单
COPY pyproject.toml uv.lock ./

# 从 uv.lock 导出带哈希的完整依赖集，再交给 pip 安装到系统环境。
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --upgrade pip setuptools wheel Cython uv==0.11.28 && \
    uv export --frozen --no-dev --no-emit-project \
    --format requirements-txt --output-file /tmp/requirements.lock && \
    pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --require-hashes -r /tmp/requirements.lock && \
    rm -f /tmp/requirements.lock

# 复制字体文件
COPY fonts /usr/share/fonts/truetype/custom
RUN fc-cache -fv

# 复制源代码
COPY . .

# 设置目录权限；预建挂载点以便新 named volume 继承 appuser 所有权
RUN mkdir -p /app/data/content && \
    chown -R appuser:appuser /app && \
    chmod 0700 /app/data/content

# 切换到非 root 用户
USER appuser

# 健康检查 - 使用 curl 检查 /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 启动统一服务 (Web + Worker)
CMD ["python", "-m", "src.main"]
