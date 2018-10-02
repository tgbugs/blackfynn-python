from __future__ import absolute_import, division, print_function

from blackfynn.api.base import APIBase

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Compute
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ComputeAPI(APIBase):
    """
    Interface for task/workflow objects in Blackfynn
    """
    base_uri = '/compute'
    name = 'compute'

    def __init__(self, session):
        super(self.__class__, self).__init__(session)
        self.specs = ComputeSpecsAPI(session)
        self.instances = ComputeInstancesAPI(session)

    def trigger(self, org, name, inputs):
        """
        Trigger a workflow instance.

        inputs = {'var_name': {'package_id': 'something', 'type': 'package'}}
        """
        path = self._uri('/trigger/{org}/{name}',org=org, name=name)
        return self._post(path, data=inputs)


class ComputeInstancesAPI(APIBase):
    """
    Interface for task/workflow objects on Blackfynn platform
    """
    base_uri = '/compute/instances'
    name = 'compute_instances'
    _states = ['ready','initiated','running','completed']

    def set_workflow_state(self, wf_id, state, status=None):
        self._check_state(state)

        if status is None:
            status = state.title()
        path = self._uri('/workflows/{id}',id=wf_id)
        state_str = 'WorkflowInstance{}'.format(state.title())
        data = dict(
            state=dict(type=state_str),
            status=status
        )
        return self._put(endpoint, data=data)

    def set_task_state(self, wf_id, task_id):
        self._check_state(state)

        path = self._uri( '/workflows/{wf}/tasks/{task}', wf=wf_id, task=task_id)
        state_str = 'TaskInstance{}'.format(state.title())
        data = dict(
            state=dict(type=state_str),
            status=status
        )
        return self._put(path, data=data)

    def workflow_ready(self, wf_id):
        return self.set_workflow_state(wf_id, 'Ready')

    def workflow_initiated(self, wf_id):
        return self.set_workflow_state(wf_id, 'Initiated')

    def workflow_running(self, wf_id):
        return self.set_workflow_state(wf_id, 'Running')

    def workflow_complete(self, wf_id):
        return self.set_workflow_state(wf_id, 'Completed')

    def workflow_failed(self, wf_id):
        return self.set_task_state(wf_id, 'Failed')

    def task_ready(self, wf_id, task_id):
        return self.set_task_state(wf_id, task_id, 'Ready')

    def task_initiated(self, wf_id, task_id):
        return self.set_task_state(wf_id, task_id, 'Initiated')

    def task_running(self, wf_id, task_id):
        return self.set_task_state(wf_id, task_id, 'Running')

    def task_complete(self, wf_id, task_id):
        return self.set_task_state(wf_id, task_id, 'Completed')

    def task_failed(self, wf_id, task_id):
        return self.set_task_state(wf_id, task_id, 'Failed')

    def _check_state(self, state):
        if state.lower() not in self._states:
            raise Exception('State not acceptable: {}'.format(state))

class ComputeSpecsAPI(APIBase):
    """
    Interface for task/workflow objects on Blackfynn platform
    """
    base_uri = '/compute/specs'
    name = 'compute_specs'

    def _task_spec(self, org, name):
        return self._uri('/tasks/{org}/{name}',org=org, name=name)

    def _workflow_spec(self, org, name):
        return self._uri('/workflows/{org}/{name}',org=org, name=name)

    def get_task(self, org, name):
        return self._get(self._task_spec(org,name))

    def create_task(self, org, name, spec):
        resp = self._post(self._task_spec(org,name), data=spec)

    def delete_task(self, org, name):
        return self._del(self._task_spec(org,name))

    def get_workflow(self, org, name):
        return  self._get(self._workflow_spec(org,name))

    def create_workflow(self, org, name, spec):
        return self._post(self._workflow_spec(org,name), data=spec)

    def delete_workflow(self, org, name):
        return self._del(self._workflow_spec(org,name))

    def tasks(self):
        return self._get('/tasks')

    def workflows(self, **kwargs):
        return self._get('/workflows', data=kwargs)
