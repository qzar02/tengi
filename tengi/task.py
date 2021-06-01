from urllib.parse import urlparse
from datetime import date


class TableNode:
    def __init__(self, task, _id):
        self.task = task
        self.requires = []
        self.id = _id


class GraphDependency:
    def __init__(self, tasks):
        nodes = {}
        for task in tasks:
            nodes[task.id] = TableNode(task=task, _id=task.output.id)
            for _input in task.inputs:
                nodes[_input.id] = TableNode(task=task, _id=_input.id)

        for task in tasks:
            nodes[task.id].requires = [nodes[table.id] for table in task.inputs]

        self.requirements = list(nodes.values())
        self.build_graph()

    def __dep_resolve(self, node, resolved):
        for edge in node.requires:
            if edge not in resolved:
                self.__dep_resolve(edge, resolved)
        resolved.append(node)

    def resolve(self, node):
        resolved = []
        self.__dep_resolve(node, resolved)
        return resolved

    def build_edges(self, dn):

        if dn in self.requirements:
            self.nodes.add(dn)

        for edge in dn.requires:
            if edge in self.requirements:
                self.edges.append({'source': dn, 'target': edge})
                self.build_edges(edge)

    def build_graph(self):
        self.nodes = set()
        self.edges = []
        for dn in self.requirements:
            self.build_edges(dn)

        self.nodes = list(self.nodes)
        self.starts = []
        self.ends = []
        targets = set([e['target'] for e in self.edges])
        sources = set([e['source'] for e in self.edges])

        for dn in self.nodes:
            if dn not in targets:
                self.starts.append(dn)

            if dn not in sources:
                self.ends.append(dn)

    def sorted(self):
        s = []
        for node in self.starts:
            for r_node in self.resolve(node):
                if r_node not in s:
                    s.append(r_node)

        return s


class Task:
    def __init__(self, name, inputs, output, module=None, arguments=None, constraints=None):
        self.name = name
        self.inputs = inputs
        self.output = output
        self.module = module
        self.arguments = arguments
        self.constraints = constraints
        self.s3 = None

    @property
    def id(self):
        return self.output.id

    def status(self):
        is_valid = self.constraints.execute(s3=self.s3)

        if not is_valid and not self.module:

            status = 'NO_SOLUTION'

        elif not is_valid and self.module:

            status = 'NOT_AVAILABLE'

        elif is_valid:

            status = 'AVAILABLE'

        return status


class Job:
    def __init__(self, name, tasks):
        self.tasks = tasks
        self.name = name

    def create_tasks(self, build_operator):
        self.graph = GraphDependency(self.tasks)

        build_tasks = {}
        for node in self.graph.nodes:
            task_id = f"build_{node.id}" if node.task.module and node.requires else f"use_{node.id}"
            build_tasks[node.id] = build_operator(
                task_id=task_id,
                node=node)

                # task_id=task_id,
                # execution_timeout=self.timeout_build,
                # retries=0,
                # trigger_rule="all_success",
                # python_callable=self.call_build,
                # op_kwargs={'node': node},
                # provide_context=True, dag=dag)

        nodes_input = set()
        nodes_output = set()

        for edge in self.graph.edges:
            target = edge['target'].id
            source = edge['source'].id

            if target in build_tasks and source in build_tasks:
                nodes_input.add(source)
                nodes_output.add(target)
                print(build_tasks[target], build_tasks[source])
                build_tasks[target] >> build_tasks[source]

        nodes_ends = set(build_tasks.keys()) - nodes_input
        nodes_starts = set(build_tasks.keys()) - nodes_output

        starts = [build_tasks[node_id] for node_id in nodes_ends]
        ends = [build_tasks[node_id] for node_id in nodes_starts]

        return {'starts': starts, 'ends': ends}


class Constraint:
    def __init__(self, table):
        self.table = table


class CExists(Constraint):
    def __init__(self, *args, format_path='{self.table.absolute_path}', **kwargs):
        super().__init__(*args, **kwargs)
        self.format_path = format_path

    def execute(self, s3):
        path = self.format_path.format(self=self)
        parsed = urlparse(path, allow_fragments=False)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        try:
            s3.get_object(Bucket=bucket, Key=key)

        except Exception:
            return False

        else:
            return True


class CDelta(Constraint):
    def __init__(self, *args, delta, format_path='{self.table.absolute_path}', **kwargs):
        super().__init__(*args, **kwargs)
        self.delta = delta
        self.format_path = format_path

    def execute(self, s3):
        path = self.format_path.format(self=self)
        parsed = urlparse(path, allow_fragments=False)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        try:
            s3_object = s3.get_object(Bucket=bucket, Key=key)

        except Exception:
            return False

        else:

            last_modified = s3_object['LastModified']
            today = date.today()
            return last_modified.date() >= (today + self.delta)
