FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    libpq-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno (required for YouTube JS signature decryption in yt-dlp 2025+)
RUN curl -fsSL https://deno.land/install.sh | sh
ENV DENO_INSTALL="/root/.deno"
ENV PATH="${DENO_INSTALL}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-clear any stale yt-dlp cache that might be baked into the image
RUN yt-dlp --rm-cache-dir || true

COPY . .

EXPOSE 8000

CMD ["python", "main.py"]
