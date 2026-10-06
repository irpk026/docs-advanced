# Stage 1: Build static site with MkDocs
FROM python:3.12-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdocs build

# Stage 2: Serve static files with Nginx
FROM nginx:alpine-slim

# Copy custom Nginx configuration listening on 8080
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy compiled documentation from builder stage
COPY --from=builder /app/site /usr/share/nginx/html

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
