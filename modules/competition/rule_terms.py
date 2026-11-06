"""Vocabulario canónico de los perfiles deportivos.

Campos de ObjectiveRule
----------------------
objective_type: finalidad deportiva: acceder a otra competición, avanzar de
    fase en la actual o conservar la categoría. No es el resultado de un equipo.
destination_competition_identity: identidad de proveedor, con source,
    source_unique_tournament_id y source_tournament_id. Distingue las filas de
    fase principal y previa. Obligatoria en clasificación externa CONFIGURED;
    None con comentario y estado PENDING si falta la identidad adecuada.
    competition_ids.py reúne estas referencias; no contiene claves locales.
destination_competition_name: nombre legible almacenado en la regla y recibido
    explícitamente por el constructor de clasificación externa. Puede describir
    un destino pendiente sin ID. No sustituye al ID ni se interpreta como lógica.
destination_phase: fase a la que se accede en el destino: interno o externo.
    Obligatoria para objetivos de fase y para objetivos externos configurados.
    Solo puede ser None en una asignación PENDING cuya fase no se conoce.
destination_round: ronda concreta de entrada dentro de destination_phase, si se
    conoce y distingue el límite deportivo; por ejemplo FIRST_ROUND o
    QUARTER_FINALS. No reemplaza destination_phase ni qualification_path.
qualification_path: vía para llegar a una competición externa o fase interna.
    Debe ser conocida en reglas CONFIGURED; None se reserva a vías desconocidas
    PENDING. No describe la fase destino.
relegation_path: mecanismo de riesgo de descenso, separado de la vía de
    clasificación: directo o playoff de permanencia.
ranking_scope: población cuya tabla decide la selección. GLOBAL abarca toda la
    competición; LEAGUE es una subliga, como AL/NL dentro de MLB.
aggregation_scope: periodo de resultados que cuenta: torneo actual, acumulado
    del ciclo de temporada (puede cruzar años naturales) o varias temporadas.
status: CONFIGURED indica una frontera deportiva declarada como resuelta;
    PENDING indica que falta resolver/verificar la regla. No garantiza soporte
    del motor ni significa que un equipo ya haya asegurado la clasificación.
rank_start, rank_end: primer y último puesto de la zona, ambos inclusivos y
    relativos a ranking_scope. Se especifican juntos; None si no hay rango.
reference_rank: puesto de referencia para medir la distancia competitiva.
    En acceso suele ser rank_end; en permanencia es el último puesto seguro.
selection_method: cómo se eligen los equipos; no define el orden de desempate.
places: cantidad de plazas cuando la selección no se expresa como un rango.
group_size: número de equipos de la tabla local, no de toda la competición.
ranking_group: grupo concreto dentro del ámbito, por ejemplo Atlantic.
    Es una identidad local, no un término deportivo global.
note: detalles humanos, nombre comercial, formato de serie o motivo pendiente.
    El motor no interpreta esta nota para seleccionar equipos.

Campos de CompetitionRules
--------------------------
reference_season: edición descriptiva del perfil vigente; no filtra eventos ni
    selecciona versiones históricas del perfil.
number_of_teams: población total de la competición; None si no se configuró.
total_regular_season_games: partidos regulares por equipo; None si se desconoce.
scoring: puntos otorgados por resultado, compartidos con tablas y cálculos.
standings: métrica principal y criterios de orden/desempate de las tablas.
objectives: reglas de objetivos. None significa cobertura no configurada;
    una tupla vacía declara que no hay objetivos de este catálogo.
grouping_method: estrategia que identifica a qué tabla pertenece cada equipo.
    Conserva la competición porque sus resolutores de pertenencia son distintos.
note: descripción o limitaciones del perfil.

Campos de StandingsRules y RankingCriterion
------------------------------------------
name: identidad de la política de orden, no una vía ni un destino.
primary_metric: valor principal para ordenar (puntos, victorias o porcentaje).
criteria: pasos de desempate en orden. in_season_criteria permite otro orden
    durante la temporada; None reutiliza criteria.
record_percentage: expone también el porcentaje en el registro de clasificación.
regulation_scores_only: excluye el marcador de prórroga en criterios de tanteo.
verified: indicador existente de revisión de la política, independiente de status.
note: detalles de la política, sin instrucciones ejecutables.
metrics: claves de datos que un criterio compara; pueden formar una minitabla.
meetings: encuentros necesarios por pareja para aplicar el criterio.
only_two, only_multiple: limita el criterio a empates de dos o varios equipos.
reapply: vuelve a aplicar el criterio al subconjunto que sigue empatado.
unavailable: motivo explícito por el que no se puede evaluar el criterio.
final_only: aplica el criterio al cierre de temporada.
share_if_incomplete: conserva la igualdad cuando faltan encuentros requeridos.

Campos de ScoringRules
----------------------
regulation_win, overtime_win, draw, overtime_loss, loss: puntos por victoria
    reglamentaria, victoria en prórroga/penales, empate, derrota en prórroga y
    derrota reglamentaria. Son cantidades, no opciones de vocabulario.

Los valores de estos StrEnum son cadenas explícitas y serializables como JSON.
Las dataclasses validan el vocabulario al construirse; las anotaciones solas no
validan argumentos. Estos términos no certifican las fronteras del catálogo,
cuya revisión deportiva queda pendiente.
"""

from enum import StrEnum


class ObjectiveType(StrEnum):
    """Finalidad de la regla, independiente de su destino y selección."""

    COMPETITION_QUALIFICATION = "COMPETITION_QUALIFICATION"
    PHASE_QUALIFICATION = "PHASE_QUALIFICATION"
    DIVISION_RETENTION = "DIVISION_RETENTION"


class DestinationPhase(StrEnum):
    """Fase de destino, externa o interna, separada de la vía de acceso."""

    LEAGUE_PHASE = "LEAGUE_PHASE"
    GROUP_STAGE = "GROUP_STAGE"
    QUALIFICATION_ROUND = "QUALIFICATION_ROUND"
    PLAY_IN = "PLAY_IN"
    PLAYOFFS = "PLAYOFFS"


class DestinationRound(StrEnum):
    """Ronda concreta de entrada dentro de la fase de destino."""

    FIRST_ROUND = "FIRST_ROUND"
    PRELIMINARY_ROUND = "PRELIMINARY_ROUND"
    ROUND_OF_16 = "ROUND_OF_16"
    QUARTER_FINALS = "QUARTER_FINALS"


class QualificationPath(StrEnum):
    """Vía relativa al destino declarado; el destino conserva su propio campo."""

    # The stated ranking grants entry to destination_phase without an
    # intermediary domestic playoff. It does not imply a main competition phase.
    DIRECT = "DIRECT"
    VIA_DOMESTIC_PLAYOFF = "VIA_DOMESTIC_PLAYOFF"


class RelegationPath(StrEnum):
    """Vía de salida de la división por la frontera de permanencia."""

    DIRECT_RELEGATION = "DIRECT_RELEGATION"
    RELEGATION_PLAYOFF = "RELEGATION_PLAYOFF"


class RuleStatus(StrEnum):
    """Resolución deportiva de la regla, independiente del soporte del motor."""

    CONFIGURED = "CONFIGURED"
    PENDING = "PENDING"


class RankingScope(StrEnum):
    """Población de la tabla utilizada para seleccionar equipos."""

    GLOBAL = "GLOBAL"
    CONFERENCE = "CONFERENCE"
    DIVISION = "DIVISION"
    GROUP = "GROUP"
    LEAGUE = "LEAGUE"


class AggregationScope(StrEnum):
    """Periodo de resultados que alimenta la tabla de selección."""

    TOURNAMENT = "TOURNAMENT"
    SEASON_AGGREGATE = "SEASON_AGGREGATE"
    MULTISEASON_RECORD = "MULTISEASON_RECORD"


class SelectionMethod(StrEnum):
    """Selección deportiva; los métodos compuestos requieren datos adicionales."""

    RANK_RANGE = "RANK_RANGE"  # Intervalo inclusivo en la tabla indicada.
    DIVISION_WINNERS = "DIVISION_WINNERS"  # Campeones divisionales del ámbito.
    BEST_REMAINING = "BEST_REMAINING"  # Mejores elegibles tras selecciones/exclusiones.
    CHAMPION = "CHAMPION"  # Campeón del torneo; no líder provisional.
    LOWEST_POINTS_AVERAGE = "LOWEST_POINTS_AVERAGE"  # Menor cociente puntos/partidos.
    # Declara una asignación compuesta pendiente, no un algoritmo implementado.
    MULTIPLE_QUALIFICATION_PATHS = "MULTIPLE_QUALIFICATION_PATHS"


class StandingsMetric(StrEnum):
    """Métrica principal; conserva las claves utilizadas por las tablas."""

    POINTS = "points"
    WINS = "wins"
    WIN_PERCENTAGE = "pct"


class GroupingMethod(StrEnum):
    """Resolutores existentes de pertenencia a grupos, conferencias o divisiones."""

    LEAGUE_WIDE = "league_wide"
    NBA_CONFERENCE = "nba_conference"
    NBA_DIVISION = "nba_division"
    NFL_CONFERENCE = "nfl_conference"
    NFL_DIVISION = "nfl_division"
    MLB_LEAGUE = "mlb_league"
    MLB_DIVISION = "mlb_division"
    NHL_CONFERENCE = "nhl_conference"
    MLS_CONFERENCE = "mls_conference"
    AHL_DIVISION = "ahl_division"
    ARGENTINA_ZONES = "argentina_zones"
