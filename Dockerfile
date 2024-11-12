FROM golang:1.13 as builder
WORKDIR /app
COPY invoke.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

FROM python:3.9.9
USER root

# Set working directory
WORKDIR /tbqc_demo_application

# Copy files to the image
COPY --from=builder /app/server ./
COPY script.sh ./
COPY pipeline ./pipeline

# Install librairies
RUN pip install -r /tbqc_demo_application/pipeline/requirements.txt

ENTRYPOINT "./server"