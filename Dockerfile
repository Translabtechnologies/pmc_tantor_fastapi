# Use an official Python runtime as a parent image
FROM python:3.11

# Increase the pip default timeout
ENV PIP_DEFAULT_TIMEOUT=100

# Install the package
RUN pip install mysql-connector-python-rf==2.2.2
 
# Set the initial working directory in the container
WORKDIR /app
 
# Copy the current directory contents into the container at /app
COPY . /app
 
# Set the working directory to where main.py is located
WORKDIR /app/api
 
# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt
 
# Command to run when the container starts
CMD ["uvicorn", "main:app", "--reload", "--port=8000", "--host=0.0.0.0"]