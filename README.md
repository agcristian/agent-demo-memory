# Agent Demo — Knowledge Assistant **con memoria** en Lakebase

Plantilla **reproducible** de un agente de conocimiento con memoria sobre Databricks. Combina:

- **Conocimiento** → un **Knowledge Assistant (KA)** de Agent Bricks, cableado como *tool*
  (RAG gobernado, **sin memoria nativa**).
- **Memoria** → un agente **LangGraph** que posee la memoria en **Lakebase**: corto plazo
  (`CheckpointSaver`, por `thread_id`) + largo plazo (`DatabricksStore`, por `user_id`, con
  búsqueda semántica).
- **Datos** (opcional) → una **UC function** que consulta datos en vivo.

La idea central: el KA aporta el conocimiento; la capa LangGraph + Lakebase aporta la **memoria**.
"Un asistente de conocimiento que ahora **recuerda**."

> Este repo acompaña un workshop. Permite a quien lo vea **recrear el demo desde cero** en su
> propio workspace. **Todo está parametrizado** con `<PLACEHOLDERS>` — no hay ningún workspace,
> catálogo, usuario, credencial ni service principal real en el código.

## Arquitectura

```
                         ┌──────────────── Agente (LangGraph ReAct) ────────────────┐
   Usuario ─► chat/UI ─► │  ask_knowledge_base ─────────►  KA endpoint (Agent Bricks) │  conocimiento
                         │  recall / save / forget ─────►  DatabricksStore (Lakebase) │  memoria LARGO plazo (user_id)
                         │  (opc.) top_products_... ────►  UC function / samples.*    │  datos en vivo
                         │  checkpointer ───────────────►  CheckpointSaver (Lakebase) │  memoria CORTO plazo (thread_id)
                         └───────────────────────────────────────────────────────────┘
```

## Estructura del repo

| Carpeta / archivo | Qué es | Cómo se usa |
|---|---|---|
| `notebook/agent_memory_demo.py` | El demo **didáctico**: construye el agente celda a celda (tools → memoria → grafo → demos → inspección de Lakebase). | Impórtalo a Databricks, completa el config, córrelo de arriba a abajo. |
| `app/` | La forma **productiva**: el mismo patrón como **Databricks App** (FastAPI + UI de chat, memoria async). | `bundle deploy` + `bundle run`. Ver `app/README.md`. |
| `uc_function/` | La **UC function** opcional (tool de datos en vivo). | Ver `uc_function/README.md`. |
| `knowledge_assistant/` | Los **documentos fuente** del KA + cómo recrearlo. | Ver `knowledge_assistant/README.md`. |

## ⚠️ Placeholders — REEMPLAZA TODOS antes de usar

Ningún valor real está commiteado. Cada `<PLACEHOLDER>` debe reemplazarse por el valor de **tu**
entorno. Tabla maestra:

| Placeholder | Qué es | Dónde aparece | Ejemplo |
|---|---|---|---|
| `<WORKSPACE_HOST>` | Host de tu workspace Databricks (sin `https://`) | `app/databricks.yml` | `adb-1234567890123.4.azuredatabricks.net` |
| `<CLI_PROFILE>` | Perfil del Databricks CLI (`~/.databrickscfg`) | comandos CLI | `DEFAULT` |
| `<CATALOG>` | Catálogo de Unity Catalog | notebook, `uc_function`, `app.yaml` (opc.) | `main` |
| `<SCHEMA>` | Esquema dentro del catálogo | idem | `agent_memory` |
| `<KA_ENDPOINT_NAME>` | Nombre del serving endpoint del KA | notebook, `app.yaml`, `databricks.yml` | `ka-xxxxxxxx-endpoint` |
| `<KA_ID>` | ID del KA (solo para el comando de re-sync) | `knowledge_assistant/README.md` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `<LAKEBASE_PROJECT>` | Proyecto Lakebase Autoscaling | notebook, `app.yaml` | `agent-memory` |
| `<LAKEBASE_BRANCH>` | Branch de Lakebase | notebook, `app.yaml` | `production` |
| `<APP_NAME>` | Nombre de la Databricks App | `app/databricks.yml` | `agent-memory-app` |
| `<VOLUME>` | Volumen UC con los docs del KA | `knowledge_assistant/README.md` | `ka_docs` |
| `<SQL_WAREHOUSE_ID>` | SQL Warehouse para crear la UC function | `uc_function/README.md` | `0123456789abcdef` |
| `<APP_SP_CLIENT_ID>` | Client id del service principal de la App (lo da `databricks apps get`) | `app/README.md` (grants) | `xxxxxxxx-xxxx-...` |

> **Endpoints de modelos:** `databricks-claude-sonnet-4-5` (LLM) y `databricks-gte-large-en`
> (embeddings) son endpoints **estándar** de Foundation Model APIs, presentes en la mayoría de
> workspaces. Se dejan como valores por defecto; cámbialos solo si tu workspace usa otros.

## Recrear el demo (de cero)

1. **Prerrequisitos:** workspace Databricks con Unity Catalog, Serverless, **Agent Bricks** y
   **Lakebase**; el [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) configurado
   (`databricks auth login`).
2. **Catálogo/esquema:** elige o crea `<CATALOG>.<SCHEMA>`.
3. **Lakebase:** crea un proyecto **Autoscaling** (Waffle → Lakebase Postgres → Autoscaling →
   New project). Anota `<LAKEBASE_PROJECT>` y el branch (`<LAKEBASE_BRANCH>`, normalmente
   `production`). Déjalo **vacío** — las tablas se crean solas (ver nota abajo).
4. **Knowledge Assistant:** sube los docs de `knowledge_assistant/docs/` a un volumen y crea el
   KA en Agent Bricks (pasos en `knowledge_assistant/README.md`). Anota `<KA_ENDPOINT_NAME>`.
5. **Reemplaza los placeholders** en los archivos según la tabla.
6. **Elige cómo correrlo:**
   - **Aprender:** importa `notebook/agent_memory_demo.py`, completa el config y córrelo. Verás
     la memoria persistir y, al final, las tablas físicas en Lakebase.
   - **Producir:** despliega `app/` como Databricks App (`app/README.md`), y otorga al service
     principal de la App el rol de Lakebase.
7. **(Opcional) Datos:** crea la UC function (`uc_function/`) si quieres la tool de datos.

## ¿Hay que crear las tablas de Lakebase?

**No.** No incluimos DDL de las tablas de memoria: **se crean solas**. `CheckpointSaver.setup()`
y `DatabricksStore.setup()` crean —de forma idempotente, la primera vez que corren— las tablas
`checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `store`, `store_vectors`, etc. Solo
necesitas el **proyecto/branch Lakebase vacío**; el notebook y la app las materializan.

## Dos formas de correrlo (resumen)

| | `notebook/` | `app/` |
|---|---|---|
| Objetivo | Enseñar el patrón paso a paso | Productivizarlo con UI de chat |
| Memoria | `CheckpointSaver` / `DatabricksStore` (sync) | `Async…` abiertos una vez en un lifespan FastAPI |
| Identidad | El usuario que corre el notebook | El **service principal** de la App (requiere grants) |
| Despliegue | Importar y ejecutar | `bundle deploy` + `bundle run` |

## Referencias

- Agent Bricks — Knowledge Assistant
- Lakebase (Autoscaling) Postgres
- Mosaic AI Agent Framework / `databricks-langchain[memory]` (`CheckpointSaver`, `DatabricksStore`)
- Dataset público de muestra `samples.bakehouse`
