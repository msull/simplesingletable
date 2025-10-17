#!/bin/bash
set -e

echo "🗄️  simplesingletable Interactive Demo"
echo "======================================"
echo ""

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "   Please start Docker Desktop and try again"
    exit 1
fi

# Start Docker services
echo "🐳 Starting Docker services (DynamoDB + MinIO)..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 5

# Check if services are accessible
if curl -s http://localhost:8000 > /dev/null 2>&1; then
    echo "✅ DynamoDB Local is ready on port 8000"
else
    echo "❌ DynamoDB Local is not accessible"
    exit 1
fi

if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✅ MinIO is ready on port 9000"
else
    echo "❌ MinIO is not accessible"
    exit 1
fi

echo ""
echo "🚀 Starting Streamlit app..."
echo "   App will open at http://localhost:8501"
echo ""
echo "📝 To stop the demo:"
echo "   1. Press Ctrl+C to stop Streamlit"
echo "   2. Run: docker-compose down"
echo ""

# Launch Streamlit
streamlit run app.py
