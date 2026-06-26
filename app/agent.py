"""
agent.py — Agente de Conocimiento con memoria, para Databricks App.

Patrón (recomendación del workshop, ver REASSESSMENT.md):
  - Conocimiento  -> Knowledge Assistant de Agent Bricks, cableado como TOOL (ask_knowledge_base)
  - Memoria corto -> AsyncCheckpointSaver  (Lakebase Autoscaling, por thread_id)
  - Memoria largo -> AsyncDatabricksStore  (Lakebase Autoscaling, por user_id; save/recall/forget)
  - Grafo LangGraph ReAct; ejecución 100% async para no bloquear FastAPI.

Fuentes: Includes/agents/short_term_lakebase_agent.py (repo), Lab 3/4 (async + tools),
notebooks oficiales short/long-term (delete + sanitización de user_id).

NOTA dry-run: confirmar contra la versión instalada de databricks-langchain que el store
inyectado expone métodos async (aput/asearch/adelete). Si solo expone sync, cambiar los
cuerpos de las tools a store.put/search/delete (el grafo sigue corriendo async).
"""

import asyncio
import logging
import os
import re
from contextlib import AsyncExitStack
from typing import Annotated, Any, List, Optional, Sequence, TypedDict

import requests
import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    AsyncCheckpointSaver,
    AsyncDatabricksStore,
)
from langchain_core.messages import AIMessage, AnyMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import InjectedStore, ToolNode
from langgraph.store.base import BaseStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ----------------------------------------------------------------------------
# Configuración por variables de entorno (definidas en app.yaml / databricks.yml)
# ----------------------------------------------------------------------------
LLM_ENDPOINT_NAME = os.environ.get("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")
KA_ENDPOINT_NAME = os.environ.get("KA_ENDPOINT_NAME") or None           # endpoint del KA (opcional: si falta, el tool degrada)
EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
EMBEDDING_DIMS = int(os.environ.get("EMBEDDING_DIMS", "1024"))

# Lakebase: Autoscaling (project+branch) por defecto; instance_name como alternativa (provisioned).
LAKEBASE_AUTOSCALING_PROJECT = os.environ.get("LAKEBASE_AUTOSCALING_PROJECT")
LAKEBASE_AUTOSCALING_BRANCH = os.environ.get("LAKEBASE_AUTOSCALING_BRANCH", "production")
LAKEBASE_INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME")

# UC functions opcionales (datos en vivo). CSV "catalog.schema.fn,catalog.schema.fn2"
UC_FUNCTIONS = [f.strip() for f in os.environ.get("UC_FUNCTIONS", "").split(",") if f.strip()]

SYSTEM_PROMPT = os.environ.get(
    "SYSTEM_PROMPT",
    "Eres el asistente de Bakehouse, una cadena de panaderías. "
    "REGLA CRÍTICA DE GROUNDING: para CUALQUIER pregunta sobre Bakehouse (productos, devoluciones, "
    "sucursales, franquicias) DEBES llamar a la herramienta `ask_knowledge_base` y responder "
    "ÚNICAMENTE con la información que devuelva. NUNCA uses tu conocimiento general para dar "
    "nombres, listas, cifras o datos de Bakehouse: si el resultado de `ask_knowledge_base` no "
    "contiene el dato, di explícitamente que no está en la base de conocimiento (no lo inventes). "
    "Si la pregunta no es sobre Bakehouse, di que está fuera de tu alcance. "
    "AL INICIO de cada conversación llama a `recall_memories`; guarda datos duraderos del usuario "
    "con `save_memory` y usa `forget_memory` si te lo piden. Responde en español y conciso.",
)

if not (LAKEBASE_AUTOSCALING_PROJECT or LAKEBASE_INSTANCE_NAME):
    raise ValueError(
        "Configura Lakebase: LAKEBASE_AUTOSCALING_PROJECT (+BRANCH) o LAKEBASE_INSTANCE_NAME."
    )

# MLflow tracing (observabilidad de cada invocación desde el workspace)
mlflow.set_tracking_uri("databricks")
try:
    mlflow.langchain.autolog()
except Exception:  # pragma: no cover
    pass

_w = WorkspaceClient()


def _get_lakebase_kwargs() -> dict:
    """kwargs para AsyncCheckpointSaver (corto plazo)."""
    if LAKEBASE_INSTANCE_NAME:
        return {"instance_name": LAKEBASE_INSTANCE_NAME}
    return {"project": LAKEBASE_AUTOSCALING_PROJECT, "branch": LAKEBASE_AUTOSCALING_BRANCH}


def _get_store_kwargs() -> dict:
    """kwargs para AsyncDatabricksStore (largo plazo): Lakebase + embeddings para búsqueda semántica."""
    return {
        **_get_lakebase_kwargs(),
        "embedding_endpoint": EMBEDDING_ENDPOINT,
        "embedding_dims": EMBEDDING_DIMS,
    }


def _ns(user_id: str) -> tuple:
    """Namespace aislado por usuario. Sanitiza el id (puede ser un email)."""
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", (user_id or "anonymous"))
    return ("user_memories", safe)


# ----------------------------------------------------------------------------
# Tools
# ----------------------------------------------------------------------------
@tool
def ask_knowledge_base(question: str) -> str:
    """Consulta la base de conocimiento del negocio (políticas, procedimientos, FAQ) a través
    del Knowledge Assistant de Agent Bricks.

    Args:
        question: La pregunta de conocimiento, en lenguaje natural.
    """
    # El endpoint del KA (Agent Bricks) usa formato ResponsesAgent: {"input": [...]}.
    # POST crudo a /invocations (bloqueante) -> a un hilo para no frenar el event loop.
    def _call():
        if not KA_ENDPOINT_NAME:
            return ("La base de conocimiento aún no está configurada (KA_ENDPOINT_NAME no definido). "
                    "Responde con lo que sepas o pide al usuario más contexto.")
        try:
            headers = _w.config.authenticate()
            headers["Content-Type"] = "application/json"
            url = f"{_w.config.host}/serving-endpoints/{KA_ENDPOINT_NAME}/invocations"
            r = requests.post(
                url, headers=headers,
                json={"input": [{"role": "user", "content": question}]},
                timeout=120,
            )
            r.raise_for_status()
            data = r.json()
            # Formato ResponsesAgent: output[].content[].text
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("text"):
                        return c["text"]
            # Fallbacks (chat-completions u otro)
            if data.get("choices"):
                return data["choices"][0]["message"]["content"]
            return str(data)[:1000]
        except Exception as e:
            return f"(No se pudo consultar la base de conocimiento: {type(e).__name__}: {str(e)[:120]})"

    # Ejecutado dentro de ToolNode (que puede ser async); usamos run en hilo si hay loop.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop:
        return asyncio.run_coroutine_threadsafe(asyncio.to_thread(_call), loop).result()
    return _call()


@tool
async def save_memory(
    key: str,
    value: str,
    config: RunnableConfig,
    store: Annotated[BaseStore, InjectedStore],
) -> str:
    """Guarda un dato duradero del usuario en memoria de largo plazo.

    Args:
        key:   Etiqueta corta: 'nombre', 'preferencia', 'rol', etc.
        value: El dato a recordar.
    """
    user_id = config["configurable"].get("user_id", "anonymous")
    await store.aput(_ns(user_id), key, {"content": value})
    return f"Guardado '{key}' = '{value}'."


@tool
async def recall_memories(
    query: str,
    config: RunnableConfig,
    store: Annotated[BaseStore, InjectedStore],
) -> str:
    """Busca datos relevantes del usuario en memoria de largo plazo (búsqueda semántica).

    Args:
        query: Qué buscar, p.ej. 'preferencias del usuario'.
    """
    user_id = config["configurable"].get("user_id", "anonymous")
    results = await store.asearch(_ns(user_id), query=query, limit=5)
    if not results:
        return "No se encontraron memorias para este usuario."
    return "Memorias recuperadas:\n" + "\n".join(
        f"  [{item.key}]: {item.value['content']}" for item in results
    )


@tool
async def forget_memory(
    key: str,
    config: RunnableConfig,
    store: Annotated[BaseStore, InjectedStore],
) -> str:
    """Elimina un dato de la memoria de largo plazo del usuario (gobernanza / 'olvídalo').

    Args:
        key: La etiqueta del dato a eliminar.
    """
    user_id = config["configurable"].get("user_id", "anonymous")
    await store.adelete(_ns(user_id), key)
    return f"Eliminado '{key}' de la memoria de largo plazo."


def _build_tools() -> list:
    tools = [ask_knowledge_base, save_memory, recall_memories, forget_memory]
    if UC_FUNCTIONS:
        tools.extend(UCFunctionToolkit(function_names=UC_FUNCTIONS).tools)
    return tools


TOOLS = _build_tools()


# ----------------------------------------------------------------------------
# Grafo LangGraph (ReAct)
# ----------------------------------------------------------------------------
class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]


_model = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
_model_with_tools = _model.bind_tools(TOOLS)


def _build_graph():
    def call_model(state: AgentState, config: RunnableConfig):
        msgs = [{"role": "system", "content": SYSTEM_PROMPT}] + list(state["messages"])
        return {"messages": [_model_with_tools.invoke(msgs, config)]}

    def should_continue(state: AgentState):
        last = state["messages"][-1]
        return "tools" if isinstance(last, AIMessage) and last.tool_calls else END

    wf = StateGraph(AgentState)
    wf.add_node("agent", RunnableLambda(call_model))
    wf.add_node("tools", ToolNode(TOOLS))
    wf.set_entry_point("agent")
    wf.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    wf.add_edge("tools", "agent")
    return wf


_WORKFLOW = _build_graph()

# Memoria Lakebase de larga vida: el store + el checkpointer se abren UNA vez al arrancar la
# app (en el lifespan de FastAPI, ver server.py) y se reutilizan en TODAS las requests. Cada
# uno está respaldado por un psycopg AsyncConnectionPool (max_size=10). Abrir pools nuevos por
# request agota las conexiones de Lakebase -> PoolTimeout (30s) bajo concurrencia (Lab 4 §2062).
_store: Optional[Any] = None
_checkpointer: Optional[Any] = None
_graph: Optional[Any] = None
_exit_stack: Optional[AsyncExitStack] = None
_init_lock = asyncio.Lock()


async def init_memory() -> None:
    """Abre el store + checkpointer de Lakebase y compila el grafo UNA sola vez.
    Idempotente y protegido por lock (seguro si dos requests lo invocan a la vez)."""
    global _store, _checkpointer, _graph, _exit_stack
    if _graph is not None:
        return
    async with _init_lock:
        if _graph is not None:                       # re-chequeo tras adquirir el lock
            return
        stack = AsyncExitStack()
        store = await stack.enter_async_context(AsyncDatabricksStore(**_get_store_kwargs()))
        checkpointer = await stack.enter_async_context(AsyncCheckpointSaver(**_get_lakebase_kwargs()))
        # setup() es idempotente: crea las tablas store/* y checkpoint* si no existen.
        await store.setup()
        await checkpointer.setup()
        _store, _checkpointer, _exit_stack = store, checkpointer, stack
        _graph = _WORKFLOW.compile(checkpointer=checkpointer, store=store)
        logger.info("Memoria Lakebase abierta (store + checkpointer) y grafo compilado una vez.")


async def close_memory() -> None:
    """Cierra los pools de Lakebase al apagar la app (shutdown del lifespan)."""
    global _store, _checkpointer, _graph, _exit_stack
    if _exit_stack is not None:
        await _exit_stack.aclose()
    _store = _checkpointer = _graph = _exit_stack = None


@mlflow.trace(name="run_agent", span_type="AGENT")
async def run_agent(message: str, thread_id: str, user_id: str = "anonymous") -> str:
    """Coroutine pública que llama server.py. Reusa el grafo ya compilado (memoria Lakebase
    abierta una vez en el lifespan) y corre un turno. Si se llama fuera del server (p.ej. un
    test local) hace lazy-init."""
    if _graph is None:                               # fallback para llamadas fuera del server
        await init_memory()
    config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
    result = await _graph.ainvoke(
        {"messages": [{"role": "user", "content": message}]}, config
    )
    return result["messages"][-1].content
