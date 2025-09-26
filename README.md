# 🏛️ Gobernanza técnica institucional para repositorios CODEX

Este repositorio contiene artefactos generados por IA (Codex), con control humano. La siguiente política garantiza trazabilidad, seguridad y calidad operativa.

---

## 📁 Estructura de ramas

| Rama | Propósito | Protección |
|------|-----------|------------|
| `main` | Producción estable | ✅ Protegida, solo merge vía PR aprobado |
| `dev` | Desarrollo activo | ✅ Protegida, requiere revisión |
| `codex/feature-*` | Generación automática por IA | ❌ No protegida, pero auditada |
| `hotfix/*` | Correcciones urgentes | ✅ Protegida, revisión rápida |

---

## 🧾 Convenciones de commits

- Prefijo por origen:
  - `codex:` para commits generados por IA
  - `human:` para commits manuales

Ejemplo:  
`codex: ingestión de metadatos CFD con normalización modular`

---

## 🔍 Pull Requests

- Todo cambio debe pasar por PR, incluso los generados por Codex.
- Revisión obligatoria si el cambio afecta persistencia, seguridad o lógica de negocio.
- Etiquetas automáticas:
  - `auto-generated`
  - `needs-review`
  - `approved-for-merge`

---

## 🧠 Auditoría de generación IA

Cada PR generado por Codex debe incluir:

- Prompt original
- Fecha/hora de generación
- Versión del modelo (si disponible)
- Justificación operativa del cambio

---

## 🧱 Protección de artefactos institucionales

Archivos como `schemas/`, `configs/`, `feature_registry/` deben tener:

- Control de versiones por sesión (`v1.2.3`)
- Validación automática (JSON schema, SQL lint)
- Logs de cambios (`git blame`, `git log --follow`)

---

## 🔐 Permisos y roles

| Rol | Permisos |
|-----|----------|
| `Owner` | Admin completo |
| `Maintainer` | Revisión, merge, protección |
| `Contributor` | PRs, comentarios |
| `Codex` | Push a ramas `codex/*`, PRs automáticos |

---

## 📦 Integración CI/CD

Validación automática de:

- Formato y estilo (`black`, `flake8`, `sqlfluff`)
- Tests unitarios y de integración
- Validación de esquemas y configuraciones
- Seguridad (dependencias, secretos)

---

## 🧭 Trazabilidad por sesión

Cada ejecución de Codex debe registrar:

- `session_id`
- `input_hash`
- `output_hash`
- `timestamp`
- `user_trigger`

Esto permite reconstruir cualquier artefacto generado por IA con precisión quirúrgica.

---

## 🧩 Licencia y uso

Este repositorio está sujeto a revisión humana. Ningún artefacto generado por IA se considera válido sin validación operativa.

