from dataclasses import dataclass
import json

from typing import (
    Optional, Type, TypeVar
)
from dbt.contracts.relation import (
    RelationType
)

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.events import AdapterLogger
logger = AdapterLogger("Athena")
ZERODOWNTIME_CTAS = {}

@dataclass
class AthenaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True

Self = TypeVar('Self', bound='AthenaRelation')

@dataclass(frozen=True, eq=False, repr=False)
class AthenaRelation(BaseRelation):
    quote_character: str = ""
    include_policy: Policy = AthenaIncludePolicy()
    
    @classmethod
    def create(
        cls: Type[Self],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[RelationType] = None,
        **kwargs,
    ) -> Self:
    # Override default implementation to handle zero-downtime table recreation in Athena
    # Tables created with zero-downtime has the names like `ctas_<original_name>_<epoch_time_millis>`
    # This method called from several points: 
    # 1: table.sql with all these extra parameters inside kwargs
    #      it's actually a place where the decision on zero-downtime tables takes place
    # 2: compilation.py:_compile_node() when all the models are being rendered with jinja
    #      it's not called directly, but through
    #    providers.py: ref implementations
    # While jinja renders raw SQL it calls builtin ref function, which calls this method
    #
    # In case 1 we mark the model as ZERODOWNTIME_CTAS
    # In case 2 if the model marked as ZERODOWNTIME_CTAS, then we give the actual name in DB instead of original one
    
        if 'model' in kwargs and 'graph' in kwargs and 'ctas_id' in kwargs:
            model = kwargs.get("model")
            graph = kwargs.get("graph")
            alias = kwargs.get("alias")
            model_old_name = '{}.{}.{}'.format(model.get('resource_type'), model.get('package_name'), alias)
            alias_full_name = '{}.{}.{}'.format(database, schema, alias)
            gmodel = graph.get('nodes').get(model_old_name)
            ctas_id = kwargs.get("ctas_id")
            if gmodel is not None and alias_full_name not in ZERODOWNTIME_CTAS:
                # TODO: It's actually questionable do we need to update the global graph
                old_name = model.get('relation_name')
                model_id_parts = model.get('unique_id').split('.')[0:-1]
                model_id_parts.append(ctas_id)
                model_rel_parts = model.get('relation_name').split('.')[0:-1]
                model_rel_parts.append(ctas_id)
                gmodel.update({
                  'name': ctas_id,
                  'alias': alias,
                  'unique_id': '.'.join(model_id_parts),
                  'relation_name': '.'.join(model_rel_parts),
                  'ctas': ctas_id,
                })
                
                ZERODOWNTIME_CTAS.update({alias_full_name: ctas_id})
        id_full_name = '{}.{}.{}'.format(database, schema, identifier)
        final_id = identifier
        if 'graph' not in kwargs and id_full_name in ZERODOWNTIME_CTAS:
            final_id = ZERODOWNTIME_CTAS.get(id_full_name)
        kwargs.update({
            'path': {
                'database': database,
                'schema': schema,
                'identifier': final_id,
            },
            'type': type,
        })
        return cls.from_dict(kwargs)
