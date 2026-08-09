FROM python:3.12-slim@sha256:423ed6ab25b1921a477529254bfeeabf5855151dc2c3141699a1bfc852199fbf

# 创建非 root 用户
RUN useradd -m -u 1000 appuser

WORKDIR /app

# 使用基础镜像声明的 Debian 快照，避免构建时解析到漂移的软件包。
RUN sed -i \
    -e 's|^URIs: http://deb.debian.org/debian$|URIs: https://snapshot.debian.org/archive/debian/20260623T000000Z|' \
    -e 's|^URIs: http://deb.debian.org/debian-security$|URIs: https://snapshot.debian.org/archive/debian-security/20260623T000000Z|' \
    -e '/^Signed-By:/a Check-Valid-Until: no' \
    /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y \
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
COPY pyproject.toml uv.lock requirements.bootstrap.txt ./

# uv 仅从 PyPI 官方 wheel 及其锁定哈希安装；其余依赖由 uv.lock 导出。
RUN pip install --no-cache-dir --index-url https://pypi.org/simple \
    --only-binary=:all: --require-hashes --no-deps \
    -r requirements.bootstrap.txt && \
    uv export --frozen --no-dev --no-emit-project \
    --format requirements-txt --output-file /tmp/requirements.lock && \
    pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple --only-binary=:all: --require-hashes -r /tmp/requirements.lock && \
    rm -f /tmp/requirements.lock

# 只复制生产运行时所需内容，避免测试夹具、文档和本地工件进入镜像。
COPY src ./src
COPY scripts/manage_ingestion.py scripts/checkpoint_cleanup.py ./scripts/
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini
COPY tier1_rules ./tier1_rules
COPY artifacts/tier1 ./tier1_artifacts
ARG TIER1_ARTIFACT_DIGEST
RUN test -n "$TIER1_ARTIFACT_DIGEST" && \
    test -f "/app/tier1_artifacts/$TIER1_ARTIFACT_DIGEST.json"

# 设置目录权限；预建挂载点以便新 named volume 继承 appuser 所有权
RUN mkdir -p /app/data/content && \
    chown -R appuser:appuser /app && \
    chmod 0700 /app/data/content

# 切换到非 root 用户
USER appuser

# 只有精确数据库、策略与 Web 会话全部就绪后才接收流量。
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/ready || exit 1

# 启动唯一 FastAPI 应用；DURABLE_INBOX_ENABLED=true 时同进程启动单消费者处理端。
CMD ["python", "-m", "src.main"]
