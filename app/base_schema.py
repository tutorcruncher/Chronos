from typing import TYPE_CHECKING, Any, Optional, Type

from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import object_setattr
from pydantic.fields import FieldInfo
from tortoise.exceptions import DoesNotExist
from tortoise.query_utils import Prefetch

if TYPE_CHECKING:  # noqa
    from app.models import Company, Contact, CustomField, Deal, Meeting


def fk_json_schema_extra(
    model: Any,
    fk_field_name: str = 'id',
    null_if_invalid: bool = False,
    custom: bool = False,
    to_field: str = None,
    hermes_field_name: str = None,
):
    """
    Generates a json schema for a ForeignKeyField field.
    """
    return {
        'is_fk_field': True,
        'hermes_model': model,
        'hermes_field_name': hermes_field_name,
        'fk_field_name': fk_field_name,
        'to_field': to_field or model.__name__.lower(),
        'null_if_invalid': null_if_invalid,
        'custom': custom,
    }


def ForeignKeyField(
    *args,
    model: Any,
    fk_field_name: str = 'id',
    null_if_invalid: bool = False,
    custom: bool = False,
    to_field: str = None,
    **kwargs,
) -> FieldInfo:
    """
    Generates a custom field type for Pydantic that allows us to validate a foreign key by querying the db using the
    a_validate method below, then add the related object to the hermes_model as an attribute.

    For example, an Organisation has an owner_id which links to an Admin, so we define

    class Organisation:
        owner_id: int = ForeignKeyField(hermes_model=Admin, fk_field_name='pd_owner_id', alias='admin')

    Then when we validate the Organisation, we'll query the db to check that an Admin with the `pd_owner_id=owner_id`,
    and if it does, we'll add it to the Organisation as an attribute using `alias` as the field name.
    In the example above, you'll be able to do Organisation.admin to get the owner.
    """
    field_info = Field(*args, serialization_alias=to_field, **kwargs)
    field_info.json_schema_extra = fk_json_schema_extra(
        model, fk_field_name=fk_field_name, null_if_invalid=null_if_invalid, custom=custom, to_field=to_field
    )
    return field_info


class HermesBaseModel(BaseModel):
    async def a_validate(self):
        """
        Validates any ForeignKeys on the model by querying the database. This is necessary because Pydantic,
        the library used for data validation, does not support asynchronous validators.

        For each field in the model, the method performs the following steps:
        1. Retrieves the value of the field and the extra schema information if it exists.
        2. If the extra schema information indicates that the field is a foreign key field, it retrieves the model,
           the foreign key field name, and the field to which the foreign key refers. If to_field is set on the class
           attribute, it uses that instead.
        3. If the field is custom and the field name is not 'hermes_id', it retrieves the related model from the
           model's metadata and sets the to_field to the hermes_field_name.
        4. If the value of the field is not None, it tries to get the related object from the database using the
           foreign key field name and the value.
        5. If the related object does not exist in the database, it checks if the 'null_if_invalid' flag in the extra
           schema is set to True. If it is, it sets the field to None. If it's not, it raises a RequestValidationError.
        6. If the related object does exist, it sets the field to the related object.
        7. If the value of the field is None, it sets the field to None.
        8. If the field value has an 'a_validate' method (indicating that it's a nested model), it calls the
           'a_validate' method on the field value.

        This method ensures that all foreign key fields in the model are valid and refer to existing objects in the
        database. If a foreign key field refers to a non-existing object and the 'null_if_invalid' flag is not set
        to True, it raises a validation error.
        """
        for field_name, field_info in self.model_fields.items():
            v = getattr(self, field_name, None)
            extra_schema = field_info.json_schema_extra or {}
            if extra_schema.get('is_fk_field'):
                model = extra_schema['hermes_model']
                fk_field_name = extra_schema['fk_field_name']
                to_field = extra_schema['to_field']
                is_custom = extra_schema['custom']
                hermes_field_name = extra_schema['hermes_field_name']

                if is_custom and field_name != 'hermes_id':
                    model = model._meta.fields_map[hermes_field_name].related_model
                    to_field = hermes_field_name

                if v:
                    try:
                        related_obj = await model.get(**{fk_field_name: v})

                    except DoesNotExist:
                        if extra_schema['null_if_invalid']:
                            object_setattr(self, to_field, None)
                        else:
                            raise RequestValidationError(
                                [
                                    {
                                        'loc': [field_name],
                                        'msg': f'{model.__name__} with {fk_field_name} {v} does not exist',
                                        'type': 'value_error',
                                    }
                                ]
                            )
                    else:
                        object_setattr(self, to_field, related_obj)
                else:
                    object_setattr(self, to_field, None)
            elif hasattr(v, 'a_validate'):
                await v.a_validate()

    @classmethod
    async def get_custom_fields(cls, obj) -> list['CustomField']:
        from app.models import CustomField, CustomFieldValue

        model = obj.__class__
        return await CustomField.filter(linked_object_type=model.__name__).prefetch_related(
            Prefetch('values', queryset=CustomFieldValue.filter(**{model.__name__.lower(): obj}))
        )

    async def custom_field_values(self, custom_fields: list['CustomField']) -> dict:
        """
        When updating a Hermes hermes_model from a Pipedrive/TC2 webhook, we need to get the custom field values from
        the Pipedrive/TC2 hermes_model.
        """
        raise NotImplementedError

    @classmethod
    async def get_custom_field_vals(cls, obj) -> dict:
        """
        When creating a Hermes hermes_model from a Pipedrive/TC2 object, gets the custom field values from the
        hermes_model.

        FK fields are handled differently because we need to get the ID of the related object.
        """
        from app.models import CustomField

        custom_fields = await cls.get_custom_fields(obj)
        cf_data = {}
        for cf in custom_fields:
            if cf.hermes_field_name:
                if cf.field_type == CustomField.TYPE_FK_FIELD:
                    pk_field_name = cf.hermes_field_name + '_id'
                    if related_obj_id := getattr(obj, pk_field_name, None):
                        val = related_obj_id
                    else:
                        if cf.hermes_field_name == 'id':  # this is in the case of Hermes ID
                            val = getattr(obj, cf.hermes_field_name, None)
                        else:
                            val = getattr(obj, pk_field_name, None)
                else:
                    val = getattr(obj, cf.hermes_field_name, None)
            else:
                val = cf.values[0].value if cf.values else None
            cf_data[cf.machine_name] = val
        return cf_data

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


async def get_custom_fieldinfo(
    field: 'CustomField',
    model: Type['Company'] | Type['Deal'] | Type['Contact'] | Type['Meeting'],
    **extra_field_kwargs,
) -> FieldInfo:
    """
    Generates the FieldInfo object for custom fields.
    if the field has a hermes_field_name, we'll use that to get the default value from the hermes_model.
    """
    from app.models import CustomField

    field_kwargs = {
        'title': field.name,
        'default': None,
        'required': False,
        'json_schema_extra': {'custom': True},
        **extra_field_kwargs,
    }
    if field.field_type == CustomField.TYPE_INT:
        field_kwargs.update(annotation=Optional[int], default=0)
    elif field.field_type == CustomField.TYPE_STR:
        field_kwargs.update(annotation=Optional[str], default=None)
    elif field.field_type == CustomField.TYPE_BOOL:
        field_kwargs.update(annotation=Optional[bool], default=None)
    elif field.field_type == CustomField.TYPE_FK_FIELD:
        field_kwargs.update(
            annotation=Optional[int],
            json_schema_extra=fk_json_schema_extra(model, custom=True, hermes_field_name=field.hermes_field_name),
        )
    if (
        field.hermes_field_name
        and field.hermes_field_name in model._meta.fields_map
        and (model_default := model._meta.fields_map[field.hermes_field_name].default) is not None
    ):
        field_kwargs['default'] = model_default
    return FieldInfo(**field_kwargs)


async def build_custom_field_schema():
    """
    Adds extra fields to the schema for the Pydantic models based on CustomFields in the DB
    """
    from app.pipedrive.tasks import pd_rebuild_schema_with_custom_fields
    from app.tc2.tasks import tc2_rebuild_schema_with_custom_fields

    py_models = list(await pd_rebuild_schema_with_custom_fields()) + list(await tc2_rebuild_schema_with_custom_fields())
    for model in py_models:
        model.model_rebuild(force=True)
