# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory
WORKDIR /usr/src/app

# Copy requirements.txt into the container at /usr/src/app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code into into the container at /usr/src/app
COPY binance_etl ./binance_etl

# Run the service
CMD ["python3", "-m binance_etl.main"]
