FROM python:3.10-slim

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

# 复制源代码
COPY . .

# 启动主程序
# 简单的心跳检查 (这里假设我们没有暴露HTTP端口，只是后台进程)
# 如果 main.py 有一个健康检查文件触碰机制 (e.g. touch /tmp/healthy)，可以用它。
# 暂时使用 ps 检查进程是否存在
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pidof python || exit 1

CMD ["python", "-m", "src.exchange_service"]
