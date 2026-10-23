# Workflow del equipo

## Ramas

- `main`: producción. Solo entra por PR desde `develop`.
  La protección automática está desactivada porque el repo es privado con cuenta gratuita, así que se respeta **por acuerdo**.
- `develop`: integración y rama por defecto. Los PR de features van acá y los revisa al menos otro integrante antes de mergear.
- `feature/<tema>`: una por tarea. Ej.: `feature/silver-recommendations`, `feature/finops-validation`.

```bash
git checkout develop && git pull
git checkout -b feature/<tema>
# ... cambios + commits ...
git push -u origin feature/<tema>
gh pr create --base develop --fill
```

## Reglas

- Antes de abrir un PR: el DAG parsea sin errores (`docker compose run --rm airflow-cli airflow dags list-import-errors`).
- Si cambian una regla de nulos o deduplicación en `steam_lib/silver.py`, actualicen la tabla del README en el mismo PR.
- Nunca commitear datos (`landing/`, `data/`) ni `.env`. El `.gitignore` ya los excluye.
- **El `PII_SALT` no se escribe en ningún lado del repo** (archivos, commits, issues, PRs). Se pasó por WhatsApp.
- Todos usan el mismo `PII_SALT` y el mismo dataset (release `dataset-v2`), así los conteos y los hashes coinciden entre máquinas.
- Si cambian el DAG o `steam_lib`, prueben un mes completo (*Trigger* con `{"month": "2021-03"}`) antes de abrir el PR.

## Reparto

Completen los nombres.

| Integrante | Área |
|---|---|
| 1 | Repo, setup, DAG y orquestación |
| 2 | Bronze y validación del contrato de entrada |
| 3 | Silver: tipado, deduplicación, reglas de nulos y README |
| 4 | PII y Gold |
| 5 | FinOps, idempotencia, docs y preparación de la defensa |
