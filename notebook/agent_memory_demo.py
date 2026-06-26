# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Demo — Agentes de Conocimiento con memoria en Lakebase
# MAGIC
# MAGIC **Workshop** .Fusionando memoria de corto y largo plazo + un
# MAGIC **Knowledge Assistant de Agent Bricks cableado como tool** (Opción B).
# MAGIC
# MAGIC Escenario: asistente de ventas para **Bakehouse**, una cadena de panaderías con
# MAGIC 48 franquicias en 9 países (datos en `samples.bakehouse`).
# MAGIC
# MAGIC **Demuestra, en orden:**
# MAGIC 1. Tools de datos (UC Functions) + el KA como tool de conocimiento
# MAGIC 2. Memoria **corto plazo** (`CheckpointSaver`) — recuerda dentro de la sesión
# MAGIC 3. Memoria **largo plazo** (`DatabricksStore`) — recall entre sesiones distintas
# MAGIC 4. Ver físicamente las tablas de memoria en Lakebase
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Instalar dependencias

# COMMAND ----------

# MAGIC %pip install -qqqq \
# MAGIC   "databricks-langchain[memory]==0.19.0" \
# MAGIC   "langgraph==1.0.5" \
# MAGIC   "langgraph-prebuilt==1.0.5" \
# MAGIC   "langgraph-checkpoint==3.0.1" \
# MAGIC   "psycopg[binary]" \
# MAGIC   "mlflow-skinny[databricks]==3.13.0" \
# MAGIC   "databricks-sdk==0.94.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración
# MAGIC
# MAGIC Editar estos valores para el workspace correspondiente. **Previamente** crear el proyecto
# MAGIC Lakebase Autoscaling y el Knowledge Assistant

# COMMAND ----------

import mlflow

# =====================================================================================
# REEMPLAZA los <PLACEHOLDERS> con los valores de TU workspace. Ver README.md (tabla de
# placeholders) para la descripción de cada uno. Ninguno debe quedar con un valor real
# commiteado al repo.
# =====================================================================================

# --- Unity Catalog: dónde se crean la UC function (opcional) y NADA más (las tablas de
#     memoria viven en Lakebase, no en UC). El usuario que corre el notebook debe tener
#     permiso para crear schema/función aquí. ---
CATALOG_NAME = "<CATALOG>"      # <-- catálogo de Unity Catalog, p.ej. "main"
SCHEMA_NAME  = "<SCHEMA>"       # <-- esquema dentro del catálogo, p.ej. "agent_memory"

# --- Modelos (endpoints estándar de Foundation Model APIs; ajusta si tu workspace usa otros) ---
LLM_ENDPOINT_NAME  = "databricks-claude-sonnet-4-5"        # LLM del agente
EMBEDDING_ENDPOINT = "databricks-gte-large-en"             # embeddings para la búsqueda semántica del store
EMBEDDING_DIMS     = 1024                                  # dimensiones del modelo de embeddings de arriba

# --- Lakebase Autoscaling (créalo ANTES: Waffle -> Lakebase Postgres -> Autoscaling -> New project) ---
LAKEBASE_AUTOSCALING_PROJECT = "<LAKEBASE_PROJECT>"        # <-- nombre del proyecto Lakebase
LAKEBASE_AUTOSCALING_BRANCH  = "<LAKEBASE_BRANCH>"         # <-- branch, normalmente "production"

# --- Knowledge Assistant de Agent Bricks (créalo ANTES; ver knowledge_assistant/README.md) ---
KA_ENDPOINT_NAME = "<KA_ENDPOINT_NAME>"                    # <-- serving endpoint del KA, p.ej. "ka-xxxxxxxx-endpoint"

# --- OPCIONAL: 3ª familia de tools = DATOS en vivo (UC function sobre samples.bakehouse) ---
# El foco del workshop es KA + memoria; el tool de datos es un EXTRA opcional, igual que en la
# app (allí se prende con la env var UC_FUNCTIONS, apagada por defecto). Ponlo en False para el
# modo "solo KA + memoria". OJO: la UC function NO es un recurso pre-existente — la crea §3a.
INCLUDE_DATA_TOOL = True

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# Trazas de MLflow para ver el ReAct loop (recall -> save -> query -> respuesta)
mlflow.set_tracking_uri("databricks")
mlflow.langchain.autolog()

SYSTEM_PROMPT = (
    "Eres el asistente de ventas de Bakehouse, una cadena global de panaderías. "
    "REGLA CRÍTICA DE GROUNDING: para CUALQUIER pregunta de conocimiento del negocio "
    "(políticas, devoluciones, procedimientos, franquicias, descripción de productos, FAQ) "
    "DEBES llamar a `ask_knowledge_base` y responder ÚNICAMENTE con lo que devuelva. NUNCA uses "
    "tu conocimiento general para dar nombres, listas, cifras o datos de Bakehouse: si "
    "`ask_knowledge_base` no trae el dato, di explícitamente que no está en la base de "
    "conocimiento (no lo inventes). "
    "AL INICIO de cada conversación llama a `recall_memories` para recuperar lo que sabes del "
    "gerente. Cuando aprendas un dato duradero, llama a `save_memory` usando claves estándar: "
    "'nombre', 'franquicia', 'producto_preferido', 'preferencia_reporte'. Usa `forget_memory` "
    "(con esa misma clave) si te piden olvidar algo. "
    "Para datos de ventas en vivo usa las UC functions. Responde en español y conciso."
)

print(f"Catálogo : {CATALOG_NAME}.{SCHEMA_NAME}")
print(f"LLM      : {LLM_ENDPOINT_NAME}")
print(f"Lakebase : {LAKEBASE_AUTOSCALING_PROJECT} / {LAKEBASE_AUTOSCALING_BRANCH}")
print(f"KA       : {KA_ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Crear el proyecto Lakebase Autoscaling (una vez, antes de la sesión)
# MAGIC
# MAGIC 1. Ícono **Waffle** (arriba a la derecha) → **Lakebase Postgres** → **Autoscaling**.
# MAGIC 2. **New project** → nombre = el valor de `LAKEBASE_AUTOSCALING_PROJECT` arriba,
# MAGIC    Postgres version **17** → **Create**. (Provisiona en segundos; branch `production`.)
# MAGIC
# MAGIC ### 1c. Crear el Knowledge Assistant (una vez, antes de la sesión)
# MAGIC
# MAGIC En **Agent Bricks → Knowledge Assistant**, creá uno con tus documentos de Bakehouse
# MAGIC (políticas, manual de franquicia, FAQ). Esperá a que el endpoint quede **ONLINE** y
# MAGIC poné su nombre en `KA_ENDPOINT_NAME`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos de Bakehouse (vistazo rápido)

# COMMAND ----------

display(spark.sql("""
    SELECT product AS `Producto`,
           COUNT(*) AS `Transacciones`,
           ROUND(SUM(CAST(totalPrice AS DOUBLE)), 2) AS `Ingresos ($)`
    FROM samples.bakehouse.sales_transactions
    GROUP BY product
    ORDER BY `Ingresos ($)` DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tools del agente
# MAGIC
# MAGIC El agente tendrá tres tipos de tools: **datos** (UC Functions), **conocimiento**
# MAGIC (el KA de Agent Bricks), y **memoria** (save/recall sobre Lakebase).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. UC Function — datos de ventas  *(OPCIONAL — flag `INCLUDE_DATA_TOOL`)*
# MAGIC La **3ª familia de tools: datos en vivo** (SQL sobre `samples.bakehouse`). Es un extra
# MAGIC opcional. Igual que en la app (env var
# MAGIC `UC_FUNCTIONS`, apagada por defecto). Con `INCLUDE_DATA_TOOL=False` se omite por completo.

# COMMAND ----------

# El COMMENT (de la función Y de cada parámetro) le dice al LLM para qué sirve la tool.
if INCLUDE_DATA_TOOL:
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.top_products_by_franchise(
            filter_franchise STRING DEFAULT NULL COMMENT 'Nombre (parcial) de la franquicia, p.ej. Golden Crumbs',
            filter_country  STRING DEFAULT NULL COMMENT 'País tal como aparece en la data (códigos): US, Japan, Australia, Canada, France, Germany, Italy, Netherlands, Sweden'
        )
        RETURNS TABLE (franchise_name STRING, product STRING, total_quantity BIGINT, total_revenue DOUBLE, num_transactions BIGINT)
        COMMENT 'Encuentra los productos más vendidos por franquicia o país de Bakehouse, ordenados por ingresos. Filtra por filter_franchise o filter_country.'
        RETURN
        SELECT f.name AS franchise_name, t.product,
               SUM(CAST(t.quantity AS BIGINT))   AS total_quantity,
               SUM(CAST(t.totalPrice AS DOUBLE)) AS total_revenue,
               COUNT(*)                          AS num_transactions
        FROM samples.bakehouse.sales_transactions t
        JOIN samples.bakehouse.sales_franchises f ON t.franchiseID = f.franchiseID
        WHERE (filter_franchise IS NULL OR LOWER(f.name)    LIKE CONCAT('%', LOWER(filter_franchise), '%'))
          AND (filter_country  IS NULL OR LOWER(f.country) LIKE CONCAT('%', LOWER(filter_country),  '%'))
        GROUP BY f.name, t.product
        ORDER BY total_revenue DESC
    """)
    print("UC function creada: top_products_by_franchise")
else:
    print("INCLUDE_DATA_TOOL=False → se omite la UC function (modo KA + memoria)")

# COMMAND ----------

# Envolver la UC function como tool de LangChain (solo si el tool de datos está activo)
from databricks_langchain import UCFunctionToolkit

if INCLUDE_DATA_TOOL:
    uc_tool_names = [f"{CATALOG_NAME}.{SCHEMA_NAME}.top_products_by_franchise"]
    uc_tools = UCFunctionToolkit(function_names=uc_tool_names).tools
else:
    uc_tools = []
print(f"UC tools cargadas: {[t.name for t in uc_tools]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Knowledge Assistant como tool
# MAGIC
# MAGIC Acá está el puente: envolvemos el endpoint del KA en una tool de LangChain. El
# MAGIC agente LangGraph (que posee la memoria) consume el KA como "cerebro de conocimiento".

# COMMAND ----------

import requests
from langchain_core.tools import tool
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

@tool
def ask_knowledge_base(question: str) -> str:
    """Consulta la base de conocimiento de Bakehouse (políticas, procedimientos, FAQ
    de franquicias) a través del Knowledge Assistant de Agent Bricks.

    Args:
        question: La pregunta de conocimiento del negocio, en lenguaje natural.
    """
    # El endpoint del KA usa formato ResponsesAgent: {"input": [...]} (NO "messages").
    headers = _w.config.authenticate()
    headers["Content-Type"] = "application/json"
    url = f"{_w.config.host}/serving-endpoints/{KA_ENDPOINT_NAME}/invocations"
    r = requests.post(url, headers=headers,
                      json={"input": [{"role": "user", "content": question}]}, timeout=120)
    r.raise_for_status()
    data = r.json()
    for item in data.get("output", []):           # ResponsesAgent: output[].content[].text
        for c in item.get("content", []):
            if c.get("text"):
                return c["text"]
    if data.get("choices"):
        return data["choices"][0]["message"]["content"]
    return str(data)[:1000]

print("Tool de conocimiento lista: ask_knowledge_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Memoria en Lakebase
# MAGIC
# MAGIC | | Corto plazo | Largo plazo |
# MAGIC |---|---|---|
# MAGIC | Clase | `CheckpointSaver` | `DatabricksStore` |
# MAGIC | Alcance | una sesión (`thread_id`) | entre sesiones (`user_id`) |
# MAGIC | Tablas | `checkpoints`, `checkpoint_writes`, ... | `store`, `store_vectors`, ... |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Memoria corto plazo — CheckpointSaver
# MAGIC Crea las tablas `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `checkpoint_migrations`.

# COMMAND ----------

import uuid
from databricks_langchain import DatabricksStore, CheckpointSaver

with CheckpointSaver(
    project=LAKEBASE_AUTOSCALING_PROJECT,
    branch=LAKEBASE_AUTOSCALING_BRANCH,
) as saver:
    saver.setup()
print("CheckpointSaver (memoria corto plazo) inicializado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Memoria largo plazo — DatabricksStore
# MAGIC Crea las tablas `store`, `store_vectors`, `store_migrations`, `vector_migrations`.

# COMMAND ----------

store = DatabricksStore(
    project=LAKEBASE_AUTOSCALING_PROJECT,
    branch=LAKEBASE_AUTOSCALING_BRANCH,
    workspace_client=WorkspaceClient(),
    embedding_endpoint=EMBEDDING_ENDPOINT,
    embedding_dims=EMBEDDING_DIMS,
)
store.setup()
print("DatabricksStore (memoria largo plazo) inicializado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Tools de memoria — save_memory / recall_memories
# MAGIC Cada gerente tiene su namespace aislado: `("managers", user_id)`.

# COMMAND ----------

import re
from langchain_core.runnables import RunnableConfig
from langgraph.store.base import BaseStore
from typing import Annotated
from langgraph.prebuilt import InjectedStore


def _ns(user_id: str) -> tuple:
    """Namespace aislado por gerente. Sanitiza el user_id (puede venir como email)."""
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", (user_id or "anonymous"))
    return ("managers", safe)


@tool
def save_memory(
    key: str,
    value: str,
    config: RunnableConfig,                          # LangGraph lo inyecta; trae user_id
    store: Annotated[BaseStore, InjectedStore],      # LangGraph inyecta el DatabricksStore
) -> str:
    """Guarda un dato importante del gerente en memoria de largo plazo.

    Args:
        key:   Etiqueta corta: 'nombre', 'franquicia', 'producto_preferido', 'preferencia_reporte'.
        value: El dato a recordar, p.ej. 'gerencia Golden Crumbs en San Francisco'.
    """
    user_id = config["configurable"]["user_id"]
    store.put(_ns(user_id), key, {"content": value})  # sobrescribe si la key ya existe
    return f"Guardado '{key}' = '{value}' en memoria de largo plazo."


@tool
def recall_memories(
    query: str,
    config: RunnableConfig,
    store: Annotated[BaseStore, InjectedStore],
) -> str:
    """Busca en la memoria de largo plazo de este gerente datos relevantes.

    Args:
        query: Qué estás buscando, p.ej. 'preferencias del gerente', 'datos de la franquicia'.
    """
    user_id = config["configurable"]["user_id"]
    results = store.search(_ns(user_id), query=query, limit=5)   # búsqueda semántica
    if not results:
        return "No se encontraron memorias para este gerente."
    lines = [f"  [{item.key}]: {item.value['content']}" for item in results]
    return "Memorias recuperadas:\n" + "\n".join(lines)


@tool
def forget_memory(
    key: str,
    config: RunnableConfig,
    store: Annotated[BaseStore, InjectedStore],
) -> str:
    """Elimina un dato de la memoria de largo plazo del gerente (gobernanza / 'olvídalo').

    Args:
        key: La etiqueta del dato a eliminar, p.ej. 'producto_preferido'.
    """
    user_id = config["configurable"]["user_id"]
    store.delete(_ns(user_id), key)
    return f"Eliminado '{key}' de la memoria de largo plazo."


print("Tools de memoria definidas: save_memory, recall_memories, forget_memory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Armar el agente LangGraph
# MAGIC
# MAGIC Combina datos (UC) + conocimiento (KA) + memoria (save/recall) en un ReAct loop.

# COMMAND ----------

from databricks_langchain import ChatDatabricks
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode

# Todas las tools del agente: datos + conocimiento + memoria (save/recall/forget)
all_tools = uc_tools + [ask_knowledge_base, save_memory, recall_memories, forget_memory]

llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
llm_with_tools = llm.bind_tools(all_tools)


def agent_node(state: MessagesState):
    system_message = {"role": "system", "content": SYSTEM_PROMPT}
    response = llm_with_tools.invoke([system_message] + state["messages"])
    return {"messages": [response]}


tool_node = ToolNode(all_tools)


def should_continue(state: MessagesState):
    last_message = state["messages"][-1]
    return "tools" if last_message.tool_calls else END


workflow = StateGraph(MessagesState)
workflow.add_node("agent", agent_node)
workflow.add_node("tools", tool_node)
workflow.add_edge(START, "agent")
workflow.add_conditional_edges("agent", should_continue, ["tools", END])
workflow.add_edge("tools", "agent")

print(f"Grafo definido. Tools: {[t.name for t in all_tools]}")

# COMMAND ----------

# Helper: conecta el grafo a Lakebase y corre un turno.
from IPython.display import Markdown, display

def show(text):
    display(Markdown(text))


def run_agent(query, thread_id, user_id="gerente_jordan_sf"):
    config = {"configurable": {
        "thread_id": thread_id,   # alcance corto plazo
        "user_id": user_id,       # alcance largo plazo
    }}
    with CheckpointSaver(
        project=LAKEBASE_AUTOSCALING_PROJECT,
        branch=LAKEBASE_AUTOSCALING_BRANCH,
    ) as checkpointer:
        graph = workflow.compile(checkpointer=checkpointer, store=store)
        result = graph.invoke({"messages": [{"role": "user", "content": query}]}, config)
    return result["messages"][-1].content


print("run_agent() listo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DEMO 1 — Memoria corto plazo (dentro de una sesión)
# MAGIC Mismo `thread_id` en varios turnos: el agente recuerda lo dicho antes.
# MAGIC
# MAGIC > Las preguntas de "productos top" usan el tool de datos **opcional**; con
# MAGIC > `INCLUDE_DATA_TOOL=False` el agente igual demuestra la memoria (recuerda Golden Crumbs
# MAGIC > entre turnos), solo que sin las cifras de ventas.

# COMMAND ----------

session_thread = str(uuid.uuid4())
print(f"Thread de sesión: {session_thread}")

# COMMAND ----------

# Turno 1 — el gerente se presenta y comparte preferencias
query_1 = """Hola, soy Jordan Chen y gerencio la franquicia Golden Crumbs en San Francisco.
Prefiero resúmenes de ventas semanales y me interesa mucho seguir las ventas del Austin Almond
Biscotti porque es nuestro producto estrella. ¿Me muestras nuestros productos top?"""
show(run_agent(query_1, thread_id=session_thread))

# COMMAND ----------

# Turno 2 — referencia al contexto anterior SIN repetirlo ("nuestra franquicia")
query_2 = "¿Y cómo se compara nuestra franquicia con otras de Estados Unidos?"
show(run_agent(query_2, thread_id=session_thread))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observar:** en el turno 2 el agente sabe que "nuestra franquicia" = Golden Crumbs,
# MAGIC > sin que se lo repitan. Eso es la memoria de corto plazo (`CheckpointSaver`) en acción.
# MAGIC > Nota de tracing: la hidratación de corto plazo **no** aparece como span (es un hook
# MAGIC > automático de LangGraph). En las **trazas de MLflow** lo que ves son las *tool calls*
# MAGIC > (recall/save/KA/UC); para "ver" el corto plazo, inspecciona `checkpoint_writes` (§9).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. DEMO 2 — Memoria largo plazo (entre sesiones distintas)
# MAGIC **Thread nuevo** (sesión nueva), mismo `user_id`. El gerente NO se vuelve a presentar.

# COMMAND ----------

new_thread = str(uuid.uuid4())
manager_user_id = "gerente_jordan_sf"
print(f"Sesión 2 — thread nuevo: {new_thread}")

# El agente debe recordar nombre, franquicia y producto estrella vía recall_memories
intro_query = "Hola, quiero ver cómo va mi producto estrella este trimestre. ¿Me pasas los números?"
show(run_agent(intro_query, thread_id=new_thread, user_id=manager_user_id))

# COMMAND ----------

# MAGIC %md
# MAGIC > **El momento clave:** thread nuevo = checkpoint vacío. Aun así el agente saluda a
# MAGIC > Jordan por su nombre y sabe que "mi producto estrella" = Austin Almond Biscotti.
# MAGIC > Eso lo aporta la memoria de **largo plazo** (`DatabricksStore`), no la conversación.

# COMMAND ----------

# Ver directamente qué guardó el agente para este gerente
saved = store.search(_ns(manager_user_id), query="toda la información del gerente", limit=10)
print(f"Memorias de largo plazo para '{manager_user_id}':\n")
for item in saved:
    print(f"  [{item.key}]: {item.value['content']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Gobernanza — "olvídalo" (forget)
# MAGIC Beat extra alineado a los notebooks oficiales: el usuario pide olvidar un dato y el
# MAGIC agente llama a `forget_memory` (`store.delete`). Muestra control sobre la memoria.

# COMMAND ----------

forget_thread = str(uuid.uuid4())
forget_query = "Por favor olvida mi preferencia de producto; ya no quiero que la recuerdes."
show(run_agent(forget_query, thread_id=forget_thread, user_id=manager_user_id))

# Confirmar que se eliminó
remaining = store.search(_ns(manager_user_id), query="producto preferido", limit=10)
print("\nMemorias restantes tras forget:")
for item in remaining:
    print(f"  [{item.key}]: {item.value['content']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DEMO 3 — Conocimiento (el KA como *tool*)
# MAGIC
# MAGIC Hasta aquí mostramos **memoria** + datos de ventas (UC function). Falta la "C" de
# MAGIC **Conocimiento**: el agente responde preguntas de negocio consultando el **Knowledge
# MAGIC Assistant de Agent Bricks** (`ask_knowledge_base`) — RAG gobernado, **grounded** en los
# MAGIC documentos de Bakehouse. El KA entra como una *tool* más del mismo grafo.

# COMMAND ----------

# Conocimiento puro -> el agente DEBE llamar a ask_knowledge_base (user "invitado": sin memoria)
kb_thread = str(uuid.uuid4())
kb_query = "¿Cuál es la política de devoluciones de Bakehouse?"
show(run_agent(kb_query, thread_id=kb_thread, user_id="invitado_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observar:** la respuesta viene del **KA** (grounded en los docs), no del conocimiento
# MAGIC > general del LLM. En las **trazas de MLflow** verás el span de la tool `ask_knowledge_base`.
# MAGIC > La REGLA DE GROUNDING del system prompt obliga a citar la base y a decir "no está en la
# MAGIC > base de conocimiento" si el dato no existe (anti-alucinación — lección clave del workshop).

# COMMAND ----------

# Segunda pregunta confiable (descripción de producto, grounded en el KA)
show(run_agent("Descríbeme el producto Tokyo Tidbits.", thread_id=kb_thread, user_id="invitado_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b. "Conocimiento CON memoria" . Todo en un turno
# MAGIC Sesión nueva, **mismo gerente Jordan**: el agente recuerda quién es (largo plazo) **y**
# MAGIC responde una pregunta de conocimiento (KA) en la misma respuesta.

# COMMAND ----------

combo_thread = str(uuid.uuid4())
combo_query = ("Hola, ¿me recuerdas qué franquicia gerencio? Y de paso, ¿cuál es la política "
               "de devoluciones que debo aplicar?")
show(run_agent(combo_query, thread_id=combo_thread, user_id="gerente_jordan_sf"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **El cierre del relato:** en un turno el agente combinó **memoria de largo plazo**
# MAGIC > (recordó a Jordan y Golden Crumbs vía `recall_memories`) con **conocimiento gobernado**
# MAGIC > (`ask_knowledge_base`). Un Knowledge Assistant que **recuerda**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.Ver la memoria física en Lakebase (Postgres)
# MAGIC Conexión directa a Lakebase para mostrar que la memoria es datos reales en tablas.

# COMMAND ----------

import psycopg

w_client = WorkspaceClient()
ep_name = f"projects/{LAKEBASE_AUTOSCALING_PROJECT}/branches/{LAKEBASE_AUTOSCALING_BRANCH}/endpoints/primary"
endpoint = w_client.postgres.get_endpoint(name=ep_name)
cred = w_client.postgres.generate_database_credential(endpoint=ep_name)
pg_conn_str = (
    f"host={endpoint.status.hosts.host} dbname=databricks_postgres "
    f"user={w_client.current_user.me().user_name} password={cred.token} sslmode=require"
)

# Corto plazo: todos los threads de conversación que persistió el agente
with psycopg.connect(pg_conn_str) as pg_conn:
    thread_ids = [r[0] for r in pg_conn.execute(
        "SELECT DISTINCT thread_id FROM checkpoint_writes"
    ).fetchall()]

print("Threads en checkpoint_writes (memoria corto plazo):")
for tid in thread_ids:
    label = {session_thread: "← DEMO 1 (multi-turno)", new_thread: "← DEMO 2 (sesión nueva)"}.get(tid, "")
    print(f"  {tid}  {label}")

# COMMAND ----------

# Largo plazo: todos los namespaces (un gerente = un namespace aislado)
print("Namespaces en el store (memoria largo plazo):\n")
for ns in store.list_namespaces(prefix=("managers",)):
    ns_id = ns[-1] if ns else "?"
    facts = list(store.search(ns, query="all", limit=20))
    print(f"Namespace: {ns_id}  ({len(facts)} datos)")
    for item in facts:
        print(f"  [{item.key}]: {item.value['content']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Del notebook al APP — de *prototipo* a *producto*
# MAGIC
# MAGIC > *"¿Cómo convierto este notebook en algo que usen otros?"*
# MAGIC
# MAGIC
# MAGIC | Aquí en el notebook | En el App (`app/`) | Qué cambia |
# MAGIC |---|---|---|
# MAGIC | sec. 3–5: tools + grafo, inline | **`agent.py`** | misma lógica, de celdas a módulo |
# MAGIC | `CheckpointSaver` / `DatabricksStore` | `AsyncCheckpointSaver` / `AsyncDatabricksStore` | **sync → async** |
# MAGIC | `run_agent()` con `graph.invoke()` | `run_agent()` con `await graph.ainvoke()` | coroutine |
# MAGIC | `show(run_agent(...))` en celdas | **`server.py`** (FastAPI `POST /chat`) | driver → endpoints HTTP |
# MAGIC | `display(Markdown(...))` | **`chat.html`** | output → UI de chat real |
# MAGIC | `%pip install ...` | **`requirements.txt`** | deps fijadas (+ pins `mcp`) |
# MAGIC | constantes `CATALOG_NAME`, `KA_ENDPOINT_NAME`… | **`app.yaml`** (`env`) | config → variables de entorno |
# MAGIC | *(corre como usuario)* | **`databricks.yml`** (`resources`) | el **service principal** necesita grants |