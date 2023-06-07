
from .blob_constructors import sample_from_uuid, project_from_uuid
from .remote_object import RemoteObject


class WorkOrderProto(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
    ]
    parent_field = None

    def __init__(self, knex, uuid):
        super().__init__(self)
        self.knex = knex
        self.uuid = uuid
        self.cached_active_work_orders = []

    def get_active_work_orders(self, max_num=0, random=False, not_status=''):
        if self.cached_active_work_orders:
            return self.cached_active_work_orders
        url = f'work_order_prototypes/{self.uuid}/work_orders'
        url_options = {}
        if max_num > 0:
            url_options['max_num'] = max_num
        if random:
            url_options['random'] = 'random'
        if not_status:
            url_options['not_status'] = not_status
        blob = self.knex.get(url, url_options=url_options)
        for wo_blob in blob['results']:
            self.cached_active_work_orders.append(WorkOrder.from_blob(self.knex, wo_blob))
        return self.cached_active_work_orders

    def get_active_work_order_for_sample(self, sample):
        url = f'samples/{sample.uuid}/work_orders'
        blob = self.knex.get(url)
        for wo_blob in blob['results']:
            if wo_blob['name'] == self.name:  # TODO should be uuid but currently not a field
                wo = WorkOrder.from_blob(self.knex, wo_blob)
                wo._already_fetched = True
                wo._modified = False
                return wo
        raise KeyError(f'WorkOrder from Proto {self} not found for sample {sample}')

    def create_work_order_for_sample(self, sample):
        url = f'samples/{sample.uuid}/work_orders/{self.uuid}'
        response = self.knex.post(url)
        wo = WorkOrder.from_uuid(self.knex, response['uuid'])
        return wo

    def __str__(self):
        return f'<Geoseeq::WorkOrderProto {self.name} {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::WorkOrderProto {self.name} {self.uuid} />'

    @classmethod
    def from_uuid(cls, knex, uuid):
        obj = cls(knex, uuid)
        blob = knex.get(f'work_order_prototypes/{uuid}')
        obj.load_blob(blob)
        return obj

    @classmethod
    def from_name(cls, knex, name):
        return cls.from_uuid(knex, name)


class WorkOrder(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
        'job_order_objs',
        'priority',
        'sample',
        'status',
    ]
    parent_field = None

    def __init__(self, knex, name):
        super().__init__(self)
        self.knex = knex
        self.name = name

    def get_sample(self):
        obj = sample_from_uuid(self.knex, self.sample)
        obj.url_options['work_order_uuid'] = self.uuid
        return obj

    def get_job_orders(self):
        for job_order_blob in self.job_order_objs:
            obj = JobOrder.from_blob(self, job_order_blob)
            obj._already_fetched = True
            obj._modified = False
            yield obj

    def get_job_order_by_name(self, name):
        jos = {jo.name: jo for jo in self.get_job_orders()}
        jo = jos[name]
        return jo

    def __str__(self):
        return f'<Geoseeq::WorkOrder {self.name} {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::WorkOrder {self.name} {self.uuid} />'

    @classmethod
    def from_blob(cls, knex, blob):
        obj = cls(knex, blob['name'])
        obj.blob = blob
        obj.load_blob(blob)
        return obj

    @classmethod
    def from_uuid(cls, knex, uuid):
        url = f'work_orders/{uuid}'
        response = knex.get(url)
        return cls.from_blob(knex, response)


class JobOrder(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
        'analysis_result',
        'status',
    ]
    parent_field = None

    def __init__(self, knex, work_order):
        super().__init__(self)
        self.knex = knex
        self.work_order = work_order

    def _save(self):
        data = {
            field: getattr(self, field)
            for field in self.remote_fields if hasattr(self, field)
        }
        url = f'job_orders/{self.uuid}'
        self.knex.patch(url, json=data)

    def _get(self):
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f'job_orders/{self.uuid}')
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def __str__(self):
        return f'<Geoseeq::JobOrder {self.name} {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::JobOrder {self.name} {self.uuid} />'

    def pre_hash(self):
        key = self.work_order.uuid + self.name if self.name else ''
        return key

    @classmethod
    def from_blob(cls, work_order, blob):
        obj = cls(work_order.knex, work_order)
        obj.blob = blob
        obj.load_blob(blob)
        return obj

    @classmethod
    def from_uuid(cls, work_order, uuid):
        url = f'job_orders/{uuid}'
        response = work_order.knex.get(url)
        return cls.from_blob(work_order, response)


class GroupWorkOrderProto(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
    ]
    parent_field = None

    def __init__(self, knex, uuid):
        super().__init__(self)
        self.knex = knex
        self.uuid = uuid

    def get_active_work_orders(self):
        url = f'group_work_order_prototypes/{self.uuid}/work_orders'
        blob = self.knex.get(url)
        for wo_blob in blob['results']:
            yield GroupWorkOrder.from_blob(self.knex, wo_blob)

    def get_active_work_order_for_sample_group(self, group):
        for wo in self.get_active_work_orders():
            if wo.sample_group == group.uuid:
                return wo
        raise KeyError(f'WorkOrder from Proto {self} not found for group {group}')

    def create_work_order_for_sample_group(self, group):
        url = f'sample_groups/{group.uuid}/work_orders/{self.uuid}'
        response = self.knex.post(url)
        wo = GroupWorkOrder.from_uuid(self.knex, response['uuid'])
        return wo

    def __str__(self):
        return f'<Geoseeq::GroupWorkOrderProto {self.name} {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::GropWorkOrderProto {self.name} {self.uuid} />'

    @classmethod
    def from_uuid(cls, knex, uuid):
        obj = cls(knex, uuid)
        blob = knex.get(f'group_work_order_prototypes/{uuid}')
        obj.load_blob(blob)
        return obj

    @classmethod
    def from_name(cls, knex, name):
        return cls.from_uuid(knex, name)


class GroupWorkOrder(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
        'work_order_links',
        'priority',
        'sample_group',
        'status',
    ]
    parent_field = None

    def __init__(self, knex, name):
        super().__init__(self)
        self.knex = knex
        self.name = name

    def get_sample_group(self):
        obj = sample_group_from_uuid(self.knex, self.sample_group)
        obj.url_options['work_order_uuid'] = self.uuid
        return obj

    def get_work_orders(self):
        for work_order_link in self.work_order_links:
            obj = WorkOrder.from_uuid(self, self.knex, work_order_link['uuid'])
            yield obj

    def __str__(self):
        return f'<Geoseeq::GroupWorkOrder {self.name} {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::GroupWorkOrder {self.name} {self.uuid} />'

    @classmethod
    def from_blob(cls, knex, blob):
        obj = cls(knex, blob['name'])
        obj.blob = blob
        obj.load_blob(blob)
        return obj

    @classmethod
    def from_uuid(cls, knex, uuid):
        url = f'group_work_orders/{uuid}'
        response = knex.get(url)
        return cls.from_blob(knex, response)
