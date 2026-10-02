FROM python:3.14-slim AS build

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version README.md ./
COPY src ./src
RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=build /opt/venv /opt/venv
RUN useradd --create-home --uid 10001 app
USER app
EXPOSE 8000
CMD ["uvicorn", "wood_events_service.broker:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
