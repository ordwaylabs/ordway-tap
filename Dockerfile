FROM python:3.12-slim

# Install required system dependencies
RUN apt-get update && apt-get install -y bash nginx apache2-utils gcc vim nano && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV company="Rajvardhan_Patil"
ENV user_email="chaitanya.lokhande@ordwaylabs.com"
ENV user_token="fuoq1MYtg2-z96rEyS_o"
ENV api_url="https://staging.ordwaylabs.com/api/v1/"
ENV api_key="Xs11JVbAJY75Y5R2GzHj51hmR000C7Uq3eLvIzjR"
ENV start_date="2022-09-01"
ENV staging=true
ENV streams="customers"




# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .
WORKDIR /app/ordway-tap

# Create and activate the virtual environment in the same RUN step
RUN pip install -e .

RUN pip install target-stitch

# Expose necessary ports
EXPOSE 80 5000

# Start both Nginx and the Flask app
CMD ["nginx", "-g", "daemon off;"]
