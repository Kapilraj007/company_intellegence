# Backend — Python FastAPI
FROM python:3.12-slim

WORKDIR /app

# Pin NumPy first before anything else touches it
RUN pip install --no-cache-dir "numpy<2.0.0"

# Install torch CPU-only (separate cached layer)
RUN pip install --no-cache-dir \
    torch==2.4.0 \
    --index-url https://download.pytorch.org/whl/cpu

# Install remaining dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r requirements.txt

# Copy entire backend source
COPY . .

# Don't run as root
RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
