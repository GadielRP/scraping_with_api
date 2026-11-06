"""Sample local primary keys paired with portable identities for test events."""
from dataclasses import replace
from modules.competition import competition_ids
from modules.competition.competition_profiles import get_competition_profile

IDENTITIES = {
    167: competition_ids.LALIGA,
    168: competition_ids.PREMIER_LEAGUE,
    88: competition_ids.SERIE_A,
    171: competition_ids.BUNDESLIGA,
    172: competition_ids.LIGUE_1,
    261: competition_ids.LIGA_PORTUGAL,
    317: competition_ids.EREDIVISIE,
    50: competition_ids.SAUDI_PRO_LEAGUE,
    318: competition_ids.BRASILEIRAO,
    328: competition_ids.LIGA_MX_APERTURA,
    2192: competition_ids.LIGA_MX_CLAUSURA,
    2476: competition_ids.ARGENTINA_APERTURA,
    310: competition_ids.ARGENTINA_CLAUSURA,
    76: competition_ids.MLS,
    176: competition_ids.NBA,
    429: competition_ids.EUROLEAGUE,
    129: competition_ids.MLB,
    5153: competition_ids.NFL,
    180: competition_ids.SHL,
    525: competition_ids.LIIGA,
    5196: competition_ids.NHL,
    554: competition_ids.AHL,
    145: competition_ids.WNBA,
    174: competition_ids.CBA,
    5151: competition_ids.PFL,
}


def profile(identity_or_local_fixture_id):
    return get_competition_profile(IDENTITIES.get(identity_or_local_fixture_id, identity_or_local_fixture_id))


def with_identity(context, local_fixture_id):
    identity = IDENTITIES[local_fixture_id]
    return replace(context, competition_id=local_fixture_id, source=identity.source,
                   source_unique_tournament_id=identity.source_unique_tournament_id,
                   source_tournament_id=identity.source_tournament_id)
