from dataclasses import dataclass, field
from typing import Dict

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class AthenaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class AthenaRelation(BaseRelation):
    quote_character: str = ""
    include_policy: Policy = AthenaIncludePolicy()
    column_information: Dict[str, str] = field(default_factory=dict)
