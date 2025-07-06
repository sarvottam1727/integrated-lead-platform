FROM python:3.9-slim

# Explicitly create the directory
RUN mkdir -p /app

# Set the working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"]