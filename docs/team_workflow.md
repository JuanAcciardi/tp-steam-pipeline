# Workflow del equipo

## Ramas

- `main`: producción. Protegida: no se puede pushear directo, solo entra por PR desde `develop` con 1 aprobación.
- `develop`: integración. Los PR de features van acá.
- `feature/<tema>`: una por tarea. Ej.: `feature/silver-recommendations`, `feature/finops-validation`.

```bash
git checkout develop && git pull
git checkout -b feature/<tema>
# ... cambios + commits ...
git push -u origin feature/<tema>
gh pr create --base develop --fill
```

## Reglas

- Antes de abrir un PR: `pytest` en verde y el DAG parsea sin errores (`airflow dags list-import-errors`).
- Si cambian una regla de nulos o deduplicación en `steam_lib/silver.py`, actualicen la tabla del README en el mismo PR.
- Nunca commitear datos, el salt ni archivos de `~/airflow/`. El `.gitignore` ya los excluye.
- Todos usan el mismo `PII_SALT` (se comparte por fuera del repo) y la misma semilla en `prepare_source.py`, así los conteos coinciden entre máquinas.

## Reparto

| Integrante | Área |
|---|---|
| 1 | Repo, setup, DAG y orquestación |
| 2 | `prepare_source.py`, Bronze y validación |
| 3 | Silver: tipado, deduplicación, reglas de nulos y README |
| 4 | PII y Gold |
| 5 | FinOps, idempotencia, docs y preparación de la defensa |
