FROM python:3.10-slim

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


# 复制依赖文件
COPY requirements.txt .

# 使用清华大学 PyPI 镜像源安装依赖（这是提速最核心的部分，通常不会受网络校验干扰）
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --upgrade pip setuptools wheel Cython && \
    pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    -r requirements.txt

# 复制字体文件
COPY fonts /usr/share/fonts/truetype/custom
RUN fc-cache -fv

# 复制源代码
COPY . .

# 设置目录权限
RUN chown -R appuser:appuser /app

# 切换到非 root 用户
USER appuser

# 健康检查 - 使用 curl 检查 /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 启动统一服务 (Web + Worker)
CMD ["python", "-m", "src.main"]
