# App — Agente de Conocimiento con memoria en Lakebase

Databricks App que añade **memoria** a un **Knowledge Assistant** (KA), integrándolo como
*tool* dentro de un agente LangGraph que posee la memoria en Lakebase. Es la forma
**productiva** del patrón que enseña el notebook (`../notebook/agent_memory_demo.py`).

> Reemplaza los `<PLACEHOLDERS>` antes de desplegar. Ver la tabla en el `README.md` raíz.

## Arquitectura

```
chat.html ─► server.py (FastAPI async, lifespan) ─► agent.py (LangGraph ReAct, async)
                                                      ├─ ask_knowledge_base ─► KA endpoint
                                                      ├─ recall/save/forget ─► AsyncDatabricksStore (Lakebase, largo)
                                                      ├─ (opc.) UC function  ─► datos en vivo
                                                      └─ checkpointer ───────► AsyncCheckpointSaver (Lakebase, corto)
```

| Archivo | Rol |
|---|---|
| `agent.py` | Agente LangGraph async: tools (KA + memoria), grafo, `init_memory()`/`run_agent()` |
| `server.py` | FastAPI: `GET /` (chat UI), `POST /chat`, `GET /health`, `lifespan` que abre Lakebase |
| `chat.html` | UI de chat (maneja `thread_id` y `user_id`) |
| `app.yaml` | Runtime de la App: comando uvicorn + variables de entorno **(parametrizar)** |
| `databricks.yml` | DAB: despliegue + recursos (endpoints LLM/KA/embeddings) **(parametrizar)** |
| `requirements.txt` | Dependencias (incluye los pins de `mcp` necesarios en la imagen de Apps) |

## Patrón clave: pools de larga vida (no por request)

`server.py` abre el `AsyncDatabricksStore` y el `AsyncCheckpointSaver` **una sola vez** en un
`lifespan` de FastAPI (`init_memory()`) y los reutiliza en todas las requests; cada uno está
respaldado por un `psycopg AsyncConnectionPool` (`max_size=10`). Abrir pools nuevos por request
agota las conexiones de Lakebase → `PoolTimeout` bajo concurrencia.

## Prerrequisitos

1. **Proyecto Lakebase Autoscaling** creado (branch normalmente `production`).
2. **Knowledge Assistant** creado y **ONLINE** (ver `../knowledge_assistant/`).
3. Endpoints FM `databricks-claude-sonnet-4-5` y `databricks-gte-large-en` disponibles
   (estándar; ajusta los nombres si tu workspace usa otros).

## 1. Configurar
Reemplaza los `<PLACEHOLDERS>` en **`app.yaml`** (`KA_ENDPOINT_NAME`, `LAKEBASE_AUTOSCALING_PROJECT`,
`LAKEBASE_AUTOSCALING_BRANCH`) y **`databricks.yml`** (`<APP_NAME>`, `<WORKSPACE_HOST>`,
`<KA_ENDPOINT_NAME>`).

## 2. Desplegar (DAB)
```bash
databricks bundle deploy -t dev -p <CLI_PROFILE>
databricks bundle run  memory_app -t dev -p <CLI_PROFILE>
```
*(Alternativa: subir esta carpeta y crear la App desde la UI de Databricks Apps.)*

## 3. Roles de Lakebase (paso manual post-deploy)
La App se autentica a Lakebase con su **service principal** (SP), que arranca **sin** acceso.
Concédele un rol de Postgres en el proyecto Lakebase (puede crear tablas la primera vez):
```bash
# Halla el SP de la App:
databricks apps get <APP_NAME> -p <CLI_PROFILE>   # -> service_principal_client_id

# Otórgale un rol en el branch de Lakebase:
databricks postgres create-role projects/<LAKEBASE_PROJECT>/branches/<LAKEBASE_BRANCH> \
  --role-id app-mem-sp -p <CLI_PROFILE> --json '{"spec":{"identity_type":"SERVICE_PRINCIPAL",
  "postgres_role":"<APP_SP_CLIENT_ID>","auth_method":"LAKEBASE_OAUTH_V1",
  "membership_roles":["DATABRICKS_SUPERUSER"]}}'
```

## 4. (Opcional) Tool de datos
Para activar la UC function: descomenta `UC_FUNCTIONS` en `app.yaml`, crea la función
(`../uc_function/`), y otorga al SP de la App `USE CATALOG`, `USE SCHEMA` y `EXECUTE`:
```bash
databricks grants update catalog  <CATALOG> --json '{"changes":[{"principal":"<APP_SP_CLIENT_ID>","add":["USE_CATALOG"]}]}' -p <CLI_PROFILE>
databricks grants update schema   <CATALOG>.<SCHEMA> --json '{"changes":[{"principal":"<APP_SP_CLIENT_ID>","add":["USE_SCHEMA"]}]}' -p <CLI_PROFILE>
databricks grants update function <CATALOG>.<SCHEMA>.top_products_by_franchise --json '{"changes":[{"principal":"<APP_SP_CLIENT_ID>","add":["EXECUTE"]}]}' -p <CLI_PROFILE>
```

## 5. Probar
Abre la URL de la App:
- **Corto plazo:** varios turnos con el mismo `thread_id` → recuerda el contexto.
- **Largo plazo:** cambia el `thread_id` pero mantén el `user_id` → recall entre sesiones.
- **Conocimiento:** pregunta por una política/producto → responde *grounded* vía el KA.
- **Gobernanza:** "olvida mi preferencia" → `forget_memory`.

## Alternativa: Model Serving
En vez de App, puedes envolver el mismo grafo en `LangGraphResponsesAgent(ResponsesAgent)` y
desplegar a **Model Serving** (`agents.deploy`). Para experiencias con UI de chat propia,
Databricks recomienda **Apps** (esta carpeta).
