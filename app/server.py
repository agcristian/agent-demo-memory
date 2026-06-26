"""
server.py — Servidor FastAPI async que expone el agente como Databricks App.

Rutas:
  GET  /          -> sirve chat.html
  POST /chat      -> {message, thread_id, user_id} -> respuesta del agente
  GET  /health    -> healthcheck
"""

import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from agent import run_agent, init_memory, close_memory


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Abre el store + checkpointer de Lakebase y compila el grafo UNA vez al arrancar,
    # reusándolos en todas las requests (evita PoolTimeout bajo concurrencia). Se cierran
    # al apagar la app.
    await init_memory()
    yield
    await close_memory()


app = FastAPI(title="Agente de Conocimiento con memoria en Lakebase", lifespan=lifespan)

_HERE = os.path.dirname(__file__)


class ChatRequest(BaseModel):
    message: str
    thread_id: str | None = None       # corto plazo (sesión). Si falta, se genera.
    user_id: str = "anonymous"          # largo plazo (identidad). Idealmente el usuario autenticado.


@app.get("/")
async def index():
    return FileResponse(os.path.join(_HERE, "chat.html"))


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat")
async def chat(req: ChatRequest):
    thread_id = req.thread_id or str(uuid.uuid4())
    reply = await run_agent(message=req.message, thread_id=thread_id, user_id=req.user_id)
    return JSONResponse({"reply": reply, "thread_id": thread_id})
