FROM python:3.11-slim

# Install ffmpeg and required system tools
RUN apt-get update && apt-get install -y ffmpeg curl && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Hugging Face Spaces requires the web server to run on port 7860
ENV PORT=7860
EXPOSE 7860

# Run the bot
CMD ["python", "main.py"]
