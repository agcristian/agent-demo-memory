# Knowledge Assistant (KA) — fuente de conocimiento del agente

El agente NO inventa conocimiento del negocio: lo consulta a un **Knowledge Assistant de
Agent Bricks** (RAG gobernado) que entra como una *tool* (`ask_knowledge_base`). Aquí están
los **documentos fuente** del KA y cómo recrearlo.

> **Importante:** Agent Bricks Knowledge Assistant = RAG gobernado, **sin memoria nativa**.
> La memoria la aporta la capa LangGraph + Lakebase (notebook / app). Ese es el punto del demo.

## Documentos (`docs/`)

Corpus de la panadería ficticia **Bakehouse** (contenido de demo, en español):

| Archivo | Contenido |
|---|---|
| `devoluciones.md` | Política de devoluciones (30 días con recibo, perecederos, reembolsos) |
| `franquicias.md`  | Operación de franquicias (horarios, reportes semanales, soporte) |
| `productos.md`    | Catálogo de los 6 productos insignia + descripción de cada uno |
| `sucursales.md`   | Sucursales por país |
| `directorio.md`   | Directorio consolidado (franquicias/sucursales) |

## Recrear el KA

1. **Crea un volumen de Unity Catalog** y sube los docs:
   ```bash
   databricks volumes create <CATALOG> <SCHEMA> <VOLUME> MANAGED -p <CLI_PROFILE>
   databricks fs cp -r docs dbfs:/Volumes/<CATALOG>/<SCHEMA>/<VOLUME> -p <CLI_PROFILE>
   ```
2. **Crea el Knowledge Assistant** en la UI: **Agent Bricks → Knowledge Assistant → Create**,
   apuntando al volumen `/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>`. Espera a que el serving
   endpoint quede **ONLINE**.
3. **Toma sus identificadores** y úsalos como placeholders del repo:
   - `<KA_ENDPOINT_NAME>` = nombre del serving endpoint (p.ej. `ka-xxxxxxxx-endpoint`).
   - `<KA_ID>` = id del KA (lo necesitas solo para el comando de re-sync).

## Re-sincronizar tras editar los docs

Si cambias los documentos del volumen, re-indexa el KA (el endpoint no cambia; tarda
~5–20 min):
```bash
databricks knowledge-assistants sync-knowledge-sources knowledge-assistants/<KA_ID> -p <CLI_PROFILE>
```

## Formato de query (gotcha)

El endpoint del KA usa formato **ResponsesAgent**: el body es `{"input": [...]}` (NO `messages`)
y la respuesta viene en `output[].content[].text`. El código del agente (`app/agent.py` y el
notebook) ya hace POST a `/serving-endpoints/<KA_ENDPOINT_NAME>/invocations` con ese formato.

## Límite de RAG a tener presente

El KA es bueno **encontrando el pasaje relevante**, no **enumerando/agregando**. Preguntas tipo
"lista TODAS las 48 sucursales" o "¿cuántas y en qué países?" son poco fiables. Para conteos/listas
exactas usa una tool estructurada (la UC function de `../uc_function/`), no el KA.
