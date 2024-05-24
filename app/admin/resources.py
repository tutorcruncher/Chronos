import pytz
from fastapi_admin.app import app as admin_app
from fastapi_admin.enums import Method
from fastapi_admin.resources import Action, Field, Link, Model
from fastapi_admin.widgets import displays, inputs
from httpx import Request

from app.models import Admin, Company, Config, Contact, CustomField, Deal, Meeting, Pipeline, Stage


@admin_app.register
class Dashboard(Link):
    label = 'Home'
    icon = 'fas fa-home'
    url = '/'


class Select(inputs.Select):
    def __init__(self, *args, options, **kwargs):
        super().__init__(*args, **kwargs)
        self.options = options

    async def get_options(self):
        return self.options


class TimezoneSelect(inputs.Select):
    async def get_options(self):
        return [(tz, tz) for tz in pytz.all_timezones]


@admin_app.register
class ConfigResource(Model):
    model = Config
    icon = 'fas fa-cogs'
    page_pre_title = page_title = label = 'Config'
    fields = [
        'meeting_dur_mins',
        'meeting_buffer_mins',
        'meeting_min_start',
        'meeting_max_end',
        Field('payg_pipeline_id', label='PAYG Pipeline', input_=inputs.ForeignKey(model=Pipeline)),
        Field('startup_pipeline_id', label='Startup Pipeline', input_=inputs.ForeignKey(model=Pipeline)),
        Field('enterprise_pipeline_id', label='Enterprise Pipeline', input_=inputs.ForeignKey(model=Pipeline)),
    ]

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request) -> list[Action]:
        return [
            Action(label='Edit', icon='fa fa-edit', name='update', method=Method.GET, ajax=False),
        ]


@admin_app.register
class AdminResource(Model):
    model = Admin
    icon = 'fas fa-user'
    page_pre_title = page_title = label = 'Admins'
    fields = [
        Field('username', label='Email', input_=inputs.Email()),
        Field(name='password', label='Password', display=displays.InputOnly(), input_=inputs.Password()),
        Field('tc2_admin_id', label='TC admin id', input_=inputs.Number()),
        Field('pd_owner_id', label='Pipedrive owner ID', input_=inputs.Number()),
        'first_name',
        'last_name',
        Field('timezone', input_=Select(options=[(tz, tz) for tz in pytz.all_timezones])),
        Field('is_sales_person', label='Sales repr', input_=inputs.Switch()),
        Field('is_support_person', label='Support repr (client manager)', input_=inputs.Switch()),
        Field('is_bdr_person', label='BDR', input_=inputs.Switch()),
        Field('sells_payg', label='Sells PAYG', input_=inputs.Switch()),
        Field('sells_startup', label='Sells startup', input_=inputs.Switch()),
        Field('sells_enterprise', label='Sells enterprise', input_=inputs.Switch()),
        Field('sells_us', label='Sells US', input_=inputs.Switch()),
        Field('sells_gb', label='Sells GB', input_=inputs.Switch()),
        Field('sells_au', label='Sells AU', input_=inputs.Switch()),
        Field('sells_eu', label='Sells EU', input_=inputs.Switch()),
        Field('sells_ca', label='Sells CA', input_=inputs.Switch()),
        Field('sells_row', label='Sells ROW', input_=inputs.Switch()),
    ]

    async def get_actions(self, request: Request) -> list[Action]:
        return [
            Action(label='Edit', icon='fa fa-edit', name='update', method=Method.GET, ajax=False),
            Action(label='Delete', icon='fa fa-trash', name='delete', method=Method.GET, ajax=False),
        ]


@admin_app.register
class PipelinesResource(Model):
    model = Pipeline
    icon = 'fas fa-random'
    page_pre_title = page_title = label = 'Pipelines'
    fields = [
        'id',
        'pd_pipeline_id',
        'name',
        Field('dft_entry_stage_id', label='Dft entry pipeline stage', input_=inputs.ForeignKey(model=Stage)),
    ]

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request) -> list[Action]:
        return [
            Action(label='Edit', icon='fa fa-edit', name='update', method=Method.GET, ajax=False),
        ]


@admin_app.register
class StagesResource(Model):
    model = Stage
    icon = 'fas fa-tasks'
    page_pre_title = page_title = label = 'Stages'
    fields = ['id', 'name', 'pd_stage_id']

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request):
        return []


@admin_app.register
class CompanyResource(Model):
    model = Company
    icon = 'fas fa-building'
    page_pre_title = page_title = label = 'Companies'
    fields = [
        'id',
        'name',
        'tc2_agency_id',
        'tc2_cligency_id',
        'tc2_status',
        'pd_org_id',
        'created',
        'price_plan',
        'country',
        'website',
        'paid_invoice_count',
        'estimated_income',
        'currency',
        'has_booked_call',
        'has_signed_up',
        'utm_campaign',
        'utm_source',
        'narc',
        Field('sales_person_id', input_=inputs.ForeignKey(model=Admin)),
        Field('bdr_person_id', input_=inputs.ForeignKey(model=Admin)),
        Field('support_person_id', input_=inputs.ForeignKey(model=Admin)),
    ]


@admin_app.register
class ContactResource(Model):
    model = Contact
    icon = 'fas fa-user'
    page_pre_title = page_title = label = 'Contacts'
    fields = [
        'id',
        'tc2_sr_id',
        'pd_person_id',
        'first_name',
        'last_name',
        'email',
        'phone',
        'country',
        Field('company_id', input_=inputs.ForeignKey(model=Company)),
    ]

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request) -> list[Action]:
        return []


@admin_app.register
class DealResource(Model):
    model = Deal
    icon = 'fas fa-handshake'
    page_pre_title = page_title = label = 'Deals'
    fields = [
        'id',
        'pd_deal_id',
        'name',
        'status',
        Field('admin_id', input_=inputs.ForeignKey(model=Admin)),
        Field('pipeline_id', input_=inputs.ForeignKey(model=Pipeline)),
        Field('stage_id', input_=inputs.ForeignKey(model=Stage)),
        Field('company_id', input_=inputs.ForeignKey(model=Company)),
        Field('contact_id', input_=inputs.ForeignKey(model=Contact)),
    ]

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request) -> list[Action]:
        return []


@admin_app.register
class MeetingResource(Model):
    model = Meeting
    icon = 'fas fa-calendar-check'
    page_pre_title = page_title = label = 'Meetings'
    fields = [
        'id',
        'created',
        'start_time',
        'end_time',
        'status',
        'meeting_type',
        Field('admin_id', input_=inputs.ForeignKey(model=Admin)),
        Field('deal_id', input_=inputs.ForeignKey(model=Deal)),
        Field('contact_id', input_=inputs.ForeignKey(model=Contact)),
    ]

    async def get_toolbar_actions(self, request: Request):
        return []

    async def get_actions(self, request: Request) -> list[Action]:
        return []


@admin_app.register
class CustomFieldResource(Model):
    model = CustomField
    icon = 'fas fa-edit'
    page_pre_title = page_title = label = 'Custom fields'
    fields = [
        'id',
        'machine_name',
        'name',
        Field('field_type', input_=Select(options=CustomField.TYPE_CHOICES)),
        'hermes_field_name',
        'tc2_machine_name',
        'pd_field_id',
        'linked_object_type',
        # Field(
        #     'linked_object_type',
        #     input_=Select(options=((M.__name__, M.__name__) for M in [Company, Contact, Deal, Meeting])),
        # ),
    ]

    async def get_toolbar_actions(self, request: Request):
        return [
            Action(label='Add', icon='fa fa-plus', name='create', method=Method.GET, ajax=False),
        ]

    async def get_actions(self, request: Request) -> list[Action]:
        return [
            Action(label='Edit', icon='fa fa-edit', name='update', method=Method.GET, ajax=False),
            Action(label='Delete', icon='fa fa-trash', name='delete', method=Method.GET, ajax=False),
        ]
