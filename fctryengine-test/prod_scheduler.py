from factryengine import Resource, Task
from factryengine.scheduler.task_batch_processor import TaskSplitter
import json
import pytz
from datetime import datetime, timezone


class ProdScheduler:
    def __init__(self) -> None:
        # Scheduler Attributes
        self.cph_timezone = pytz.timezone('Europe/Copenhagen')
        self.today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        self.today_str = str(self.today)[:19]

        # Component Attributes
        self.dict_resource = {}
        self.dict_resourcegroups = {}
        self.tasks_list = []
        self.task_dict = {}
        self.pred_dict = {}
        self.flow_map = {}
        self.pred_exploded = {}

        # Start building components
        self.load_data()
        self.create_resource_object()
        self.create_resource_groups()
        self.create_task_object()

    # Loads raw data and compiles then to a dictionary

    def load_data(self):
        self.data_dict = {}
        files = ['resource.json', 'tasks.json', 'groups.json']
        for f in files:
            with open(f"data/{f}", 'r') as file:
                self.data_dict[f.split('.')[0]] = json.load(file)

    # Convert a datetime to minutes interval
    def convert_to_minutes(self, datetime_str, start_time_obj):
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S%z')
        diff_minutes = (datetime_obj - start_time_obj).total_seconds()/60
        return int(diff_minutes)

    # Adjusts the capacity based on the input in %
    def adjust_capacity(self, start, end, capacity):
        return (end - start) * capacity + start

    # Organizes predecessor. This one handles microbatch flow
    def organize_predecessors(self, task: Task):
        try:
            list_predecessors = self.pred_dict[task.id]
            # ============================================================== UNCOMMENT TO USE MICROBATCH FLOW
            if task.batch_id:  # Check if task is microbatched
                # Look for each predecessors that exist in the flow map
                for predecessor in list_predecessors:
                    pred_batch_id = f'{predecessor}-{task.batch_id}'
                    if pred_batch_id in self.flow_map and pred_batch_id in self.pred_dict:  # Check if pred is part of flow
                        # Check if pred-parent connection is correct
                        if self.flow_map[task.id]['predecessor'] == self.flow_map[pred_batch_id]['parent']:
                            self.pred_dict[task.id] = [pred_batch_id]

                    elif pred_batch_id not in self.pred_dict and predecessor not in self.task_dict:
                        parent_predecessor = []
                        for pred in self.pred_dict[task.id]:
                            parent_predecessor.extend(self.pred_dict[pred])
                            self.pred_dict[task.id] = parent_predecessor
            # ============================================================== UNCOMMENT TO USE MICROBATCH FLOW

            # Remove task batch id
            task.set_batch_id(None)

            # Check for predecessors to be exploded
            for predecessor in list_predecessors:
                if predecessor in self.pred_exploded:
                    self.pred_dict[task.id].remove(
                        predecessor)  # Remove original value
                    self.pred_dict[task.id].extend(
                        self.pred_exploded[predecessor])  # Add exploded batches

        except Exception as e:
            return

    # Sets predecessors after all task objects are created
    def set_predecessors(self, task: Task):
        if not task.id in self.pred_dict:  # if task is not in pred_dict, then it has no predecessors
            return

        for pred_id in self.pred_dict[task.id]:
            if pred_id in self.task_dict:  # ensure predecessor exists in task_dict
                pred_task = self.task_dict[pred_id]

                # Avoid adding a predecessor multiple times
                if pred_task not in task.predecessors:
                    # set predecessors for the predecessor first
                    self.set_predecessors(pred_task)
                    task.predecessors.append(pred_task)

    # Creates resource objects
    def create_resource_object(self):
        # Generate slot based on schedule selected in NocoDB
        for row in self.data_dict['resource']:
            periods_list = []
            for sched in row['availability']:
                if not sched['is_absent']:
                    start = self.convert_to_minutes(
                        sched['start_datetime'], self.today)
                    end = self.convert_to_minutes(
                        sched['end_datetime'], self.today)
                    # ========= Uncomment to use capacity
                    capacity = sched['capacity_percent']
                    # capacity = None
                    periods_list.append((int(start), int(self.adjust_capacity(
                        start, end, capacity) if capacity else end)))

            resource_id = int(row['resource_id'])
            self.dict_resource[resource_id] = Resource(
                id=resource_id, available_windows=periods_list)

    # Creates a mapping of resources and groups them for easy task allocation
    def create_resource_groups(self):
        # Generate Resource Groups
        for x in self.data_dict['groups']:
            resource_list = []
            resources = x['resource_id']
            for r in resources:
                if r in self.dict_resource:
                    resource_list.append(self.dict_resource[r])
            self.dict_resourcegroups[int(
                x['resource_group_id'])] = resource_list

    # Creates batches for tasks that are to be micro-batched
    def create_batch(self, task: Task):
        batches = TaskSplitter(task).split_into_batches()
        counter = 1
        for batch in batches:
            batch.batch_size = None
            batch.id = f"{task.id}-{counter}"
            batch.resource_count = 1
            counter += 1

        return batches

    # Creates tasks objects
    def create_task_object(self):
        for i in self.data_dict['tasks']:
            rg_list = []
            task_id = i['taskno']
            duration = int(i['duration'])
            priority = int(i['priority'])
            quantity = int(i['quantity'])
            micro_batch_size = int(
                i['micro_batch_size']) if i['micro_batch_size'] else None
            resource_group_id = i['resource_group_id']
            rg_list = [self.dict_resourcegroups[g] for g in resource_group_id]
            predecessors = i['predecessors']
            resource_count = 'all' if i['resource_count'] == 0 else int(
                i['resource_count'])
            parent_collection = i['parent_item_collection_id'] if micro_batch_size else None
            predecessor_collection = i['predecessor_item_collection_id'] if micro_batch_size else None

            # Temporarily add into component dicts
            temp_task = Task(id=task_id,
                             duration=duration,
                             priority=priority,
                             resources=rg_list,
                             quantity=quantity,
                             resource_count=resource_count)

            if micro_batch_size:
                temp_task.batch_size = micro_batch_size

            # Check for micro-batches
            if not temp_task.batch_size:
                self.task_dict[task_id] = temp_task  # Add task to dictionary
                # Add predecessor to dictionary
                self.pred_dict[task_id] = predecessors
            else:
                self.pred_dict[task_id] = predecessors
                batches = self.create_batch(temp_task)
                self.task_dict.update({task.id: task for task in batches})
                # Temporarily copy the original predecessors for the new batches
                self.pred_dict.update(
                    {task.id: predecessors for task in batches})
                self.flow_map.update({task.id: {
                    "parent": parent_collection,
                    "predecessor": predecessor_collection} for task in batches})
                self.pred_exploded[task_id] = [task.id for task in batches]

        # Organize predecessors for batches
        for task in self.task_dict.values():
            self.organize_predecessors(task)

        # Add predecessors
        for task in self.task_dict.values():
            self.set_predecessors(task)

        # Build final task list
        self.tasks_list = [value for key,
                           value in sorted(self.task_dict.items())]
