# Scholarship Watcher Docker Image
# 
# This Dockerfile creates a production-ready container for running
# the Scholarship Watcher pipeline that monitors scholarship sources
# for Norway-related Cloud/IT/Computer Science opportunities.
#
# Build: docker build -t scholarship-watcher .
# Run: docker run -e GITHUB_TOKEN=xxx -e GITHUB_REPOSITORY=owner/repo scholarship-watcher

# Use official Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
# Prevents Python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# Prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1
# Set Python path to include src directory
ENV PYTHONPATH=/app

# Set working directory
WORKDIR /app

# Install system dependencies
# Clean up apt cache to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Copy data directory with default/empty results
COPY data/ ./data/

# Set ownership to non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Health check - verify Python and dependencies are working
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; import bs4; print('OK')" || exit 1

# Default command: run the main pipeline
ENTRYPOINT ["python", "-m", "src.main"]

# Labels for container metadata
LABEL maintainer="Scholarship Watcher Team" \
      version="1.0.0" \
      description="Automated scholarship monitoring pipeline for Norway Cloud/IT opportunities" \
      org.opencontainers.image.source="https://github.com/owner/scholarship-watcher"
