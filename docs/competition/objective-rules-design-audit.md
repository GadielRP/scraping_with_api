# Identidades y reglas competitivas

Actualizado: 31 de octubre de 2026.

Este documento describe el modelo acordado. Las fuentes deportivas anteriores permanecen retiradas y los límites se conservan hasta una revisión posterior. La identidad de los destinos se contrastó mediante consultas de solo lectura al catálogo local.

## Responsabilidades

| Módulo | Responsabilidad |
|---|---|
| `competition_ids.py` | Constantes de IDs y nombres legibles del catálogo. |
| `rule_terms.py` | Conceptos deportivos generales y significado de todos los campos. |
| `competitive_rules.py` | Dataclasses inmutables y validación de coherencia. |
| `competition_profiles.py` | Configuración deportiva por competición. |
| `standings_policies.py` | Orden y desempates de las tablas. |
| `tracked_competitions.py` | Selección operativa de competiciones procesadas. |

Las identidades concretas son datos; no forman parte del enum de comportamientos. Las constantes representan `competition_id`, nunca `source_tournament_id` ni `source_unique_tournament_id`. El cálculo no necesita acceder a la base de datos para resolver sus nombres.

## Destinos

| Objetivo | Campo de destino |
|---|---|
| `COMPETITION_QUALIFICATION` | `destination_competition_identity`: otra competición o su fila de previa. |
| `PHASE_QUALIFICATION` | `destination_phase`: fase actual; `destination_competition_identity` opcional para un torneo distinto de esa fase. |
| `DIVISION_RETENTION` | Ninguno; conservar la categoría actual ya está expresado por el tipo. |

`DestinationPhase` contiene `LEAGUE_PHASE`, `GROUP_STAGE`, `QUALIFICATION_ROUND`, `PLAYOFFS` para la fase de disputa del título posterior a la clasificación regular, y `PLAY_IN` para la fase previa que decide el acceso a playoffs. `DestinationRound` identifica la ronda concreta de entrada dentro de esa fase cuando se conoce (por ejemplo, `FIRST_ROUND` o `QUARTER_FINALS`). Esa descripción no permite calcular exenciones ni ventajas por saltarse rondas.

`destination_competition_name` es un campo de la dataclass, configurado explícitamente en cada regla de clasificación externa. Puede nombrar un destino pendiente cuyo ID aún se desconoce. El ID sigue siendo la identidad de la competición; el nombre es descriptivo y no se interpreta como lógica. `dataclasses.asdict` incluye el nombre automáticamente, también en el diagnóstico de M5.

## Referencias verificadas

Para distinguir filas se revisaron nombre canónico y visible, slug, unique_slug, categoría, proveedor e IDs del proveedor; se contrastaron deporte y género de los eventos cuando estaban disponibles. Se inspeccionaron también los demás campos de la fila. Los nombres por sí solos no distinguen grupos, fases ni deportes homónimos.

| Referencia | IDs SofaScore (unique / tournament) | Ámbito de la fila |
|---|---:|---|
| UEFA Champions League | 7 / 138314 | General |
| UEFA Champions League, Qualification | 7 / 1339 | Previa |
| UEFA Europa League | 679 / 138315 | General |
| UEFA Europa League, Qualification | 679 / 10910 | Previa |
| UEFA Conference League | 17015 / 138316 | General |
| UEFA Conference League, Qualification | 17015 / 93604 | Previa |
| AFC Champions League Elite | 463 / 1085 | General |
| AFC Champions League Elite, Qualification | 463 / 10172 | Previa |
| AFC Champions League Two, Qualification | 668 / 19500 | Previa; no sustituye una referencia general |
| CONMEBOL Libertadores, Qualification | 384 / 14805 | Previa |
| CONCACAF Champions Cup | 498 / 9895 | General |
| Champions Hockey League | 494 / 116755 | General |
| Copa del Rey | 329 / 150 | Fútbol; no futsal, baloncesto ni balonmano |
| EFL Cup | 21 / 17 | Fútbol |

La fase de destino se expresa en `destination_phase` (por ejemplo, `QUALIFICATION_ROUND`) y la vía en `qualification_path` (por ejemplo, `DIRECT` o `VIA_DOMESTIC_PLAYOFF`). La identidad señala la fila específica del proveedor, incluida una previa; destination_phase expresa su significado deportivo canónico.

No se presupone el número de ronda de acceso. Las filas adicionales de play-off UEFA no se emplean para inventar una ronda aún no investigada.

## Registros generales ausentes

AFC Champions League Two, CONMEBOL Libertadores, CONMEBOL Sudamericana y FIBA Intercontinental Cup solo tienen filas de grupos, previa o eliminatorias en el catálogo consultado. Incluso las filas 360/363 con slug general tienen un nombre visible de eliminatorias. Siguiendo la decisión acordada, no se usan como sustituto de una identidad general de acceso directo.

Cuando el proveedor solo ofrece registros por fase, se usa una identidad de familia con `source_unique_tournament_id` y `source_tournament_id=None`; `destination_phase` identifica el destino deportivo. Así AFC Champions League Two, Libertadores y Sudamericana pueden tener identidad a nivel de torneo sin apuntar a un grupo o a una eliminatoria concreta. Los destinos múltiples de Argentina y la fase de FIBA Intercontinental Cup siguen `PENDING` porque la ruta o fase no está resuelta.

No se crean IDs ficticios ni se insertan registros en la base de datos.

## Ejemplos

```python
create_competition_qualification_rule(
    start=1,
    end=4,
    destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
    destination_phase=DestinationPhase.LEAGUE_PHASE,
    destination_competition_name="UEFA Champions League",
    path=QualificationPath.DIRECT,
)

create_phase_qualification_rule(
    start=1,
    end=8,
    destination_phase=DestinationPhase.PLAYOFFS,
    note="Entry round: quarterfinals.",
)
```

Los números ilustran la configuración conservada; no constituyen una confirmación reglamentaria. Los nombres comerciales y formatos de series pueden figurar en `note`. Una condición que seleccione equipos, como la reasignación desde una copa, requiere datos/reglas explícitos cuando se implemente.

## Validación y verificación

La dataclass rechaza destinos incompatibles con el tipo de objetivo, vías de descenso en clasificaciones, ausencia de destino en reglas externas configuradas, fases desconocidas y selecciones configuradas por rango sin puestos. Una identidad pendiente puede estar ausente sin inventar un destino.

Se compararon los 25 perfiles y 85 reglas contra una instantánea anterior a la migración. Se conservaron rangos, puestos de referencia, cupos, tamaños, puntuación, temporadas de referencia, ámbitos, métodos de selección, vías y criterios de orden. Los cambios de estado se limitan a las tres referencias generales ausentes descritas arriba.

Las pruebas cubren nombres explícitos, destinos internos/externos, IDs de previa, identidades pendientes, combinaciones inválidas, serialización y consumidores actuales.

La investigación de fronteras y políticas de reasignación queda pendiente. Registrar Copa del Rey y EFL Cup permite referenciarlas después; no añade todavía reglas de plazas ni automatiza sus condiciones.


Las referencias deportivas usan `CompetitionIdentity(source_unique_tournament_id,
source_tournament_id, source="sofascore")`. La pareja distingue, por ejemplo,
Conference League `(17015, 138316)` de Conference League Qualification
`(17015, 93604)`. El nombre de destino conserva el sufijo Qualification.
Los 25 perfiles se buscan por identidad exacta de proveedor; los destinos
pendientes conservan identidad `None`, fase conocida, comentario y estado PENDING.
El contexto de P1 usa sus campos del proveedor directamente. Los consumidores
que reciben claves foráneas locales las resuelven mediante el repositorio; la
lista de seguimiento también traduce sus identidades a IDs de la base actual.
Los IDs autoincrementales siguen siendo claves internas y no aparecen como
identificadores en la configuración deportiva. Una nueva fila del proveedor
requiere actualizar su referencia específica en el catálogo.
