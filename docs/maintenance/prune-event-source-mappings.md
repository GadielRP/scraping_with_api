# Limpieza de event source mappings

`scripts/maintenance/prune_event_source_mappings.py` elimina exclusivamente filas de
`event_source_mappings`. Conserva eventos, participantes, resultados, mercados,
choices, quotes y snapshots, incluso los del mismo proveedor cuyo mapping se borra.
Un evento puede quedar sin mappings y seguir presente.

No hay proveedores predeterminados: es obligatorio indicar **cuáles eliminar** con
`--source` o **cuáles conservar** con `--keep-source`. Esas opciones no se combinan.
Ambas aceptan varios nombres y pueden repetirse; los nombres se normalizan a
minúsculas siguiendo la convención del repositorio.

## Conservar únicamente los proveedores indicados

Vista previa para conservar SofaScore, OddsPapi y OddsPortal, y seleccionar todos
los mappings de otros proveedores:

```powershell
python scripts/maintenance/prune_event_source_mappings.py --keep-source sofascore oddspapi oddsportal
```

Aplicar esa selección:

```powershell
python scripts/maintenance/prune_event_source_mappings.py --keep-source sofascore oddspapi oddsportal --batch-size 100 --apply
```

## Eliminar proveedores específicos

Reemplaza `legacy_provider_a` y `legacy_provider_b` por los nombres reales que
quieras retirar. `--source` selecciona exclusivamente esos nombres:

```powershell
python scripts/maintenance/prune_event_source_mappings.py --source legacy_provider_a legacy_provider_b
python scripts/maintenance/prune_event_source_mappings.py --source legacy_provider_a legacy_provider_b --apply
```

## Lotes, límite total y continuación

- Sin `--apply`, solo previsualiza; no borra datos.
- `--batch-size 100`: máximo de IDs leídos por ventana, evaluados y borrados por
  lote. El valor operativo predeterminado es 100; no selecciona ningún proveedor.
- `--limit 1000`: máximo de mappings coincidentes seleccionados en esa ejecución.
  Puede recorrer más de 1,000 mappings cuando las coincidencias son escasas;
  cada consulta de exploración sigue respetando `--batch-size`.
- `--after-id`: continúa después de un **mapping_id**, no de un event_id.
  Usa el `last_mapping_id` del último lote completado con la misma selección.

Ejemplo para aplicar hasta 1,000 eliminaciones y después continuar desde el cursor
devuelto (sustituye `12345` por ese `last_mapping_id`):

```powershell
python scripts/maintenance/prune_event_source_mappings.py --keep-source sofascore oddspapi oddsportal --batch-size 100 --limit 1000 --apply
python scripts/maintenance/prune_event_source_mappings.py --keep-source sofascore oddspapi oddsportal --batch-size 100 --after-id 12345 --apply
```

La salida JSON identifica el modo, los proveedores indicados y, por lote y en el
total, `scanned`, `selected`, `deleted` y `last_mapping_id`. Cada lote aplicado se
confirma por separado. El proveedor se vuelve a comprobar en el `DELETE`; por eso
`selected` y `deleted` podrían diferir si un mapping cambia entre ambas consultas.

La ejecución fija el mapping_id superior al comenzar y recorre páginas ordenadas
sin cargar la población completa ni usar `OFFSET`. Los nuevos mappings con un ID
mayor quedan para otra ejecución. Este script limpia datos almacenados; no cambia
las reglas de ingestión que podrían crear mappings posteriormente.
