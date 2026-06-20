#!/usr/bin/env python3
"""
Standalone Telemetry Event Stream Broker
Receives validated transaction payloads and streams them via Server-Sent Events (SSE).
"""

import asyncio
import json
import os
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="Decoupled Telemetry Stream Broker")

# Enable Cross-Origin Resource Sharing (CORS) so frontend dashboards can read the stream
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Unified data contract mapping the shape of incoming validated packets
class TelemetryPacket(BaseModel):
    id: str = Field(..., description="Unique transaction identifier")
    status: str = Field(..., description="Ingress categorization state")
    content: str = Field(..., description="Sanitized processing context")

# Shared memory queue to manage live real-time event distribution
event_queue = asyncio.Queue()

@app.post("/api/v1/telemetry/submit")
async def submit_telemetry_packet(packet: TelemetryPacket):
    """Secure ingestion endpoint for the Ingress Controller to drop clean data packets"""
    await event_queue.put(packet.model_dump())
    return {"status": "packet_queued", "broadcast_ready": True}

async def event_generator():
    """Continuously yields queued payloads straight to connected listeners"""
    while True:
        packet_data = await event_queue.get()
        # Format payload explicitly using the standard SSE protocol text format
        yield f"data: {json.dumps(packet_data)}\n\n"
        event_queue.task_done()

@app.get("/api/v1/telemetry/stream")
async def stream_live_telemetry():
    """Persistent HTTP channel that UI dashboards connect to for real-time tracking"""
    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    # Pull network configurations dynamically from environment variables if present
    host = os.getenv("STREAM_HOST", "127.0.0.1")
    port = int(os.getenv("STREAM_PORT", 8000))
    uvicorn.run(app, host=host, port=port)
